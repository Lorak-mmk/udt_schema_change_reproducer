use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
};

use futures::{stream::FuturesUnordered, StreamExt};
use scylla_cql::frame::{
    parse_response_body_extensions,
    protocol_features::ProtocolFeatures,
    read_response_frame,
    request::{query::QueryParameters, Options, Query, SerializableRequest, Startup},
    response::result::CqlValue,
    response::Response,
    FrameParams, SerializedRequest,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpSocket, TcpStream},
};
use uuid::Uuid;

struct Connection {
    stream: TcpStream,
    features: ProtocolFeatures,
    shard: u32,
    shard_count: u32,
}

impl Connection {
    async fn connect(ip: &str, shard_info: &Option<(u32, u32)>) -> Self {
        let socket = TcpSocket::new_v4().unwrap();
        if let Some((shard, shard_count)) = shard_info {
            let mut success = false;
            for i in 1000..2000 {
                let addr = SocketAddr::new(
                    Ipv4Addr::new(0, 0, 0, 0).into(),
                    (shard_count * i + shard).try_into().unwrap(),
                );
                match socket.bind(addr) {
                    Ok(()) => {
                        success = true;
                        break;
                    }
                    _ => (),
                }
            }
            if !success {
                panic!("Can't bind port for shard {shard} ({shard_count})");
            }
            println!("Bound socket to: {}", socket.local_addr().unwrap());
        }

        let stream = socket
            .connect((ip.to_string() + ":19042").parse().unwrap())
            .await
            .unwrap();
        Connection {
            stream,
            features: ProtocolFeatures::default(),
            shard: 0,
            shard_count: 0,
        }
    }
    async fn send_request(&mut self, req: impl SerializableRequest, stream: i16) {
        let mut req = SerializedRequest::make(&req, None, false).unwrap();
        req.set_stream(stream);
        self.stream.write_all(req.get_data()).await.unwrap();
    }
    async fn read_response(&mut self) -> (FrameParams, Response) {
        let (frame_params, response_opcode, body) =
            read_response_frame(&mut self.stream).await.unwrap();
        let response_body_with_extensions =
            parse_response_body_extensions(frame_params.flags, None, body).unwrap();
        let response = Response::deserialize(
            &self.features,
            response_opcode,
            &mut &*response_body_with_extensions.body,
        )
        .unwrap();
        (frame_params, response)
    }

    async fn query(&mut self, query: &str) -> scylla_cql::frame::response::result::Result {
        self.send_request(
            Query {
                contents: std::borrow::Cow::Borrowed(query),
                parameters: QueryParameters::default(),
            },
            1,
        )
        .await;

        let (frame, resp) = self.read_response().await;
        assert_eq!(frame.stream, 1);

        match resp {
            Response::Result(result) => return result,
            r => panic!("Wrong response! {:?}", r),
        }
    }
}

async fn setup_connection(ip: &str, shard_info: Option<(u32, u32)>) -> Connection {
    let mut conn = Connection::connect(ip, &shard_info).await;

    conn.send_request(Options, 0).await;
    let (_, resp) = conn.read_response().await;
    let (shard, shard_count, features) = match resp {
        Response::Supported(supported) => {
            // println!("Supported: {supported:?}");
            let features = ProtocolFeatures::parse_from_supported(&supported.options);
            let shard: u32 = supported
                .options
                .get("SCYLLA_SHARD")
                .unwrap()
                .first()
                .unwrap()
                .parse()
                .unwrap();
            let shard_count: u32 = supported
                .options
                .get("SCYLLA_NR_SHARDS")
                .unwrap()
                .first()
                .unwrap()
                .parse()
                .unwrap();
            (shard, shard_count, features)
        }
        _ => panic!("Unexpected response"),
    };

    conn.features = features;
    conn.shard = shard;
    conn.shard_count = shard_count;

    if let Some((dst_shard, dst_shard_count)) = shard_info {
        assert_eq!(shard, dst_shard, "Connected to wrong shard");
        assert_eq!(shard_count, dst_shard_count, "Wrong shard count");
    }

    let mut options = HashMap::new();
    conn.features.add_startup_options(&mut options);
    options.insert("CQL_VERSION".to_string(), "4.0.0".to_string());
    conn.send_request(Startup { options }, 0).await;
    let (_, resp) = conn.read_response().await;
    match resp {
        Response::Ready => (),
        _ => panic!("Unexpected response"),
    };

    conn
}

async fn fetch_local_schema_version(conn: &mut Connection) -> Uuid {
    let schema_cqlval = match conn.query("SELECT schema_version FROM system.local").await {
        scylla_cql::frame::response::result::Result::Rows(rows) => {
            rows.rows[0].columns[0].as_ref().unwrap().clone()
        }
        r => panic!("Wrong response type: {:?}", r),
    };

    match schema_cqlval {
        CqlValue::Uuid(uuid) => return uuid,
        _ => panic!("Invalid type"),
    }
}

async fn verify_same_schema_all_shards(connections: &mut [Connection]) {
    let mut versions = Vec::new();
    for conn in connections.iter_mut() {
        versions.push(fetch_local_schema_version(conn).await);
    }
    if !versions.windows(2).all(|w| w[0] == w[1]) {
        panic!("Schema on single node not in agreement: {versions:?}");
    }
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args();
    let _name = args.next().unwrap();
    let ip = args.next().unwrap();
    let mut ctrl_conn = setup_connection(&ip, None).await;
    let mut shards_conns: Vec<Connection> = Vec::new();
    for shard in 0..ctrl_conn.shard_count {
        let conn = setup_connection(&ip, Some((shard, ctrl_conn.shard_count))).await;
        println!("Created connection to shard {}", conn.shard);
        shards_conns.push(conn);
    }

    println!("Drop keyspace");
    println!("{:?}", ctrl_conn.query("DROP KEYSPACE IF EXISTS ks;").await);

    println!("Create keyspace");
    println!("{:?}", ctrl_conn.query("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};").await);

    println!("Control connection - use keyspace");
    println!("{:?}", ctrl_conn.query("USE ks").await);

    println!("Create user_refs type");
    println!(
        "{:?}",
        ctrl_conn
            .query(
                "create type if not exists user_refs \
        (id text, alt_name text, firstname text, lastname text, email text)"
            )
            .await
    );

    println!("Create obs_entity type");
    println!("{:?}", ctrl_conn.query("create type if not exists obs_entity (entity_id text, version int, entity_type text, \
        type text, service text, user_refs frozen<user_refs>)").await);

    println!("Create other_entity type");
    println!(
        "{:?}",
        ctrl_conn
            .query(
                "create type if not exists other_entity (other_entity_id text, version int, \
                        entities frozen<set<text>>,type text)"
            )
            .await
    );

    println!("Create entity table");
    println!("{:?}", ctrl_conn.query("create table if not exists entity(entity_id text, other_entity_id text, \
                        entity_type text, type text, service text, entity_info frozen < obs_entity >, \
                        import_timestamp timestamp, import_timestamp_day timestamp, primary key(entity_id)\
                        ) with compact storage and \
                        compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'} \
                        and bloom_filter_fp_chance = 0.01").await);

    println!("Create index on entity(service)");
    println!(
        "{:?}",
        ctrl_conn
            .query("create index if not exists on entity(service)")
            .await
    );

    println!("Create index on entity(type)");
    println!(
        "{:?}",
        ctrl_conn
            .query("create index if not exists on entity(type)")
            .await
    );

    println!("Create entity_by_type materialized view");
    println!("{:?}", ctrl_conn.query("create materialized view if not exists entity_by_type as select * from entity where \
                    type is not null primary key(type, entity_id)").await);

    println!("Create entity_by_unique_id materialized view");
    println!("{:?}", ctrl_conn.query("create materialized view if not exists entity_by_unique_id as select * \
                     from entity where other_entity_id is not null primary key(other_entity_id, entity_id)").await);

    println!("Create entity_rel table");
    println!("{:?}", ctrl_conn.query("create table if not exists entity_rel (src_entity_id text, src_entity frozen<obs_entity>, \
                    dest_entity_id text, dest_entity frozen<obs_entity>, service text, rel_type text, \
                    deleted boolean, primary key ((src_entity_id), dest_entity_id, rel_type, service)) \
                    with compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'} \
                    and bloom_filter_fp_chance = 0.01").await);

    println!("Create index on entity_rel(dest_entity_id)");
    println!(
        "{:?}",
        ctrl_conn
            .query("create index if not exists on entity_rel(dest_entity_id)")
            .await
    );

    println!("Create index on entity_rel(service)");
    println!(
        "{:?}",
        ctrl_conn
            .query("create index if not exists on entity_rel(service)")
            .await
    );

    println!("Create index on entity_rel(rel_type)");
    println!(
        "{:?}",
        ctrl_conn
            .query("create index if not exists on entity_rel(rel_type)")
            .await
    );

    async fn insert_into_entity(connections: &mut [Connection], altered_type: bool) {
        let mut futures = FuturesUnordered::new();
        for (i, conn) in connections.iter_mut().enumerate() {
            let fut = async move {
                let new_type = if altered_type {
                    format!(", ({i}, {{{i}, {i}}})")
                } else {
                    "".to_string()
                };
                let stmt = format!("insert into ks.entity (entity_id, entity_info, entity_type, import_timestamp, \
                        import_timestamp_day, other_entity_id, service, type) values \
                        ('text{i}', ('entity_id{i}', {i}, 'entity_type{i}', 'type{i}', 'service{i}', \
                        ('id{i}', 'alt_name{i}', 'firstname{i}', 'lastname{i}', 'email{i}'{new_type}){new_type}), \
                        'entity_type{i}', 1234568979, 45621313131, 'other_entity_id{i}', 'service{i}', 'type{i}')");
                (i, conn.query(&stmt).await)
            };
            futures.push(fut);
        }

        while let Some((i, result)) = futures.next().await {
            match result {
                scylla_cql::frame::response::result::Result::Void => {
                    println!("Inserted on shard {i}")
                }
                r => panic!("Got error while inserting: {:?}", r),
            }
        }
    }

    async fn insert_into_entity_rel(connections: &mut [Connection], altered_type: bool) {
        let mut futures = FuturesUnordered::new();
        for (i, conn) in connections.iter_mut().enumerate() {
            let fut = async move {
                let new_type = if altered_type {
                    format!(", ({i}, {{{i}, {i}}})")
                } else {
                    "".to_string()
                };
                let stmt = format!("insert into ks.entity_rel (src_entity_id, src_entity, dest_entity_id, dest_entity, \
                                 service, rel_type, deleted) values \
                                 ('text{i}', ('entity_id{i}', {i}, 'entity_type{i}', 'type{i}', 'service{i}', \
                                 ('id{i}', 'alt_name{i}', 'firstname{i}', 'lastname{i}', 'email{i}'{new_type}){new_type}), \
                                 'dest_entity_id{i}', ('entity_id{i}', {i}, 'entity_type{i}', 'type{i}', 'service{i}', \
                                 ('id{i}', 'alt_name{i}', 'firstname{i}', 'lastname{i}', 'email{i}'{new_type}){new_type}), \
                                 'service{i}', 'rel_type{i}', True)");
                (i, conn.query(&stmt).await)
            };
            futures.push(fut);
        }

        while let Some((i, result)) = futures.next().await {
            match result {
                scylla_cql::frame::response::result::Result::Void => {
                    println!("Inserted on shard {i}")
                }
                r => panic!("Got error while inserting: {:?}", r),
            }
        }
    }

    insert_into_entity(&mut shards_conns, false).await;
    insert_into_entity_rel(&mut shards_conns, false).await;

    println!("Create priority_refs type");
    println!(
        "{:?}",
        ctrl_conn
            .query("create type if not exists priority_refs (priority int, description set<int>)")
            .await
    );

    println!("Alter obs_entity type");
    println!(
        "{:?}",
        ctrl_conn
            .query("alter type obs_entity add priority_refs frozen<priority_refs>")
            .await
    );

    println!("Alter user_refs type");
    println!(
        "{:?}",
        ctrl_conn
            .query("alter type user_refs add priority_refs frozen<priority_refs>")
            .await
    );

    insert_into_entity(&mut shards_conns, true).await;
    insert_into_entity_rel(&mut shards_conns, true).await;

    // assert_row_count(session=session, table_name="entity", expected=rows_num,
    //                     consistency_level=ConsistencyLevel.QUORUM)

    // assert_row_count(session=session, table_name="entity_rel", expected=rows_num,
    //                     consistency_level=ConsistencyLevel.QUORUM)
}

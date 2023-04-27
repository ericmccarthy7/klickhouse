use futures::{Stream, StreamExt};
use klickhouse::{Client, ClientOptions};

#[derive(klickhouse::Row, Debug, Default)]
pub struct TestType {
    val: u32,
}

#[tokio::test]
async fn test_same_fn_stream() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    setup(client.clone(), "test_stream_1").await;

    println!("BEFORE QUERY");
    let all_rows = client
        .query_collect::<TestType>("SELECT val FROM test_stream_1 ORDER BY val DESC;")
        .await
        .unwrap();
    println!("AFTER QUERY");

    assert!(all_rows[0].val == 54321);
    assert!(all_rows[1].val == 12345);
    assert!(all_rows[2].val == 11111);

    teardown(client, "test_stream_1").await;
}

#[tokio::test]
async fn test_sep_fn_stream() {
    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    setup(client.clone(), "test_stream_2").await;
    let mut stream = get_stream_create_client().await;

    let mut all_rows: Vec<TestType> = Vec::new();
    while let Some(res) = stream.next().await {
        all_rows.push(res.unwrap());
    }

    assert!(all_rows[0].val == 54321);
    assert!(all_rows[1].val == 12345);
    assert!(all_rows[2].val == 11111);

    teardown(client, "test_stream_2").await;
}

async fn get_stream_create_client(
) -> impl Stream<Item = Result<TestType, klickhouse::KlickhouseError>> {
    println!("BEFORE QUERY");
    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    let all_rows = client
        .query::<TestType>("SELECT val FROM test_stream_2 ORDER BY val DESC;")
        .await
        .unwrap();
    println!("AFTER QUERY");

    return all_rows;
}

#[tokio::test]
async fn test_sep_fn_stream_pass_client() {
    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    setup(client.clone(), "test_stream_3").await;
    let mut stream = get_stream_from_client(client.clone()).await;

    let mut all_rows: Vec<TestType> = Vec::new();
    while let Some(res) = stream.next().await {
        all_rows.push(res.unwrap());
    }

    assert!(all_rows[0].val == 54321);
    assert!(all_rows[1].val == 12345);
    assert!(all_rows[2].val == 11111);

    teardown(client, "test_stream_3").await;
}

async fn get_stream_from_client(
    client: Client,
) -> impl Stream<Item = Result<TestType, klickhouse::KlickhouseError>> {
    println!("BEFORE QUERY");
    let all_rows = client
        .query::<TestType>("SELECT val FROM test_stream_3 ORDER BY val DESC;")
        .await
        .unwrap();
    println!("AFTER QUERY");

    return all_rows;
}

async fn setup(client: Client, table_name: &str) {
    println!("begin setup for {}", table_name);

    client
        .execute(format!("DROP TABLE IF EXISTS {};", table_name))
        .await
        .unwrap();
    client
        .execute(format!(
            "CREATE TABLE {} (val UInt32) ENGINE=Memory;",
            table_name
        ))
        .await
        .unwrap();

    let mut block1 = TestType::default();
    let mut block2 = TestType::default();
    let mut block3 = TestType::default();

    block1.val = 12345;
    block2.val = 54321;
    block3.val = 11111;

    client
        .insert_native_block(
            format!("INSERT INTO {} (val) FORMAT NATIVE", table_name),
            vec![block1, block2, block3],
        )
        .await
        .unwrap();
}

async fn teardown(client: Client, table_name: &str) {
    println!("begin cleanup of {}", table_name);

    client
        .execute(format!("DROP TABLE IF EXISTS {};", table_name))
        .await
        .unwrap();
}

use futures::{Stream, StreamExt};
use klickhouse::{Client, ClientOptions};

#[derive(klickhouse::Row, Debug, Default)]
pub struct TestType {
    val: u32,
}

#[tokio::test]
async fn test_same_fn() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    // SETUP
    println!("begin insert into test_stream_1");

    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    client
        .execute("DROP TABLE IF EXISTS test_stream_1;")
        .await
        .unwrap();
    client
        .execute("CREATE TABLE test_stream_1 (val UInt32) ENGINE=Memory;")
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
            "INSERT INTO test_stream_1 (val) FORMAT NATIVE",
            vec![block1, block2, block3],
        )
        .await
        .unwrap();

    // TEST
    let all_rows = client
        .query_collect::<TestType>("SELECT val FROM test_stream_1 ORDER BY val DESC;")
        .await
        .unwrap();

    assert!(all_rows[0].val == 54321);
    assert!(all_rows[1].val == 12345);
    assert!(all_rows[2].val == 11111);

    // TEARDOWN
    println!("begin cleanup of test_stream_1");

    client
        .execute("DROP TABLE IF EXISTS test_stream_1;")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_separate_function() {
    // SETUP
    println!("begin cleanup into test_stream_2");

    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    client
        .execute("DROP TABLE IF EXISTS test_stream_2;")
        .await
        .unwrap();
    client
        .execute("CREATE TABLE test_stream_2 (val UInt32) ENGINE=Memory;")
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
            "INSERT INTO test_stream_2 (val) FORMAT NATIVE",
            vec![block1, block2, block3],
        )
        .await
        .unwrap();

    // TEST
    let mut all_rows: Vec<TestType> = Vec::new();
    while let Some(res) = get_client_stream().await.next().await {
        all_rows.push(res.unwrap());
    }

    assert!(all_rows[0].val == 54321);
    assert!(all_rows[1].val == 12345);
    assert!(all_rows[2].val == 11111);

    // TEARDOWN
    println!("begin cleanup of test_stream_2");

    client
        .execute("DROP TABLE IF EXISTS test_stream_2;")
        .await
        .unwrap();
}

async fn get_client_stream() -> impl Stream<Item = Result<TestType, klickhouse::KlickhouseError>> {
    let client = Client::connect("127.0.0.1:9000", ClientOptions::default())
        .await
        .unwrap();

    let all_rows = client
        .query::<TestType>("SELECT val FROM test_stream_2 ORDER BY val DESC;")
        .await
        .unwrap();

    return all_rows;
}

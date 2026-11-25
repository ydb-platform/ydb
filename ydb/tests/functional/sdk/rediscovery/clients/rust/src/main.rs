use std::{env, time::Duration};

use ydb::{AnonymousCredentials, ClientBuilder, TopicWriterMessage, TopicWriterOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::var("YDB_ENDPOINT")?;
    let database = env::var("YDB_DATABASE")?;
    let mut uri = format!("grpc://{endpoint}{database}");
    if env::var("YDB_EDS_DISCOVERY").as_deref() == Ok("1") {
        uri.push_str("?use_discovery=eds");
    }
    let client = ClientBuilder::new_from_connection_string(uri)?
        .with_credentials(AnonymousCredentials::new())
        .build()
        .await?;
    let topic = env::var("YDB_TOPIC_PATH").unwrap_or_else(|_| format!("{database}/topic"));
    match env::var("YDB_PROBE").as_deref().unwrap_or("scheme") {
        "topic-write" => {
            let writer = client
                .topic_client()
                .create_writer_with_params(
                    TopicWriterOptions::builder()
                        .topic_path(topic)
                        .producer_id(
                            env::var("YDB_PRODUCER").unwrap_or_else(|_| "eds-probe".to_owned()),
                        )
                        .build(),
                )
                .await?;
            let mut n = 0u64;
            loop {
                n += 1;
                writer
                    .write_with_ack(
                        TopicWriterMessage::builder()
                            .data(n.to_string().into_bytes())
                            .build(),
                    )
                    .await?;
                println!("OK");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        "topic-read" => {
            let consumer = env::var("YDB_CONSUMER").unwrap_or_else(|_| "probe".to_owned());
            let mut reader = client.topic_client().create_reader(consumer, topic).await?;
            loop {
                let batch = reader.read_batch().await?;
                let marker = batch.get_commit_marker();
                for mut message in batch.messages {
                    if let Some(data) = message.read_and_take().await? {
                        println!("OK {}", String::from_utf8(data)?);
                    }
                }
                reader.commit(marker)?;
            }
        }
        "scheme" => (),
        mode => return Err(format!("Unsupported YDB_PROBE {mode}").into()),
    }
    let mut scheme = client.scheme_client();
    loop {
        match scheme.list_directory(database.clone()).await {
            Ok(_) => println!("OK"),
            Err(error) => eprintln!("{error:?}"),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

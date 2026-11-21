use std::{env, time::Duration};

use ydb::{AnonymousCredentials, ClientBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::var("YDB_ENDPOINT")?;
    let database = env::var("YDB_DATABASE")?;
    let client = ClientBuilder::new_from_connection_string(&format!("grpc://{endpoint}{database}"))?
        .with_credentials(AnonymousCredentials::new())
        .build()
        .await?;
    let mut scheme = client.scheme_client();
    loop {
        match scheme.list_directory(database.clone()).await {
            Ok(_) => println!("OK"),
            Err(error) => eprintln!("{error:?}"),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

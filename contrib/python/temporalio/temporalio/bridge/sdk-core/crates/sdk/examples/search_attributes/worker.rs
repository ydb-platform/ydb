mod workflows;

use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};
use workflows::SearchAttributesWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new_assume_tokio(Default::default())?;
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let worker_options = WorkerOptions::new("search-attributes")
        .register_workflow::<SearchAttributesWorkflow>()?
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    println!("Worker started on task queue: search-attributes");
    worker.run().await?;

    Ok(())
}

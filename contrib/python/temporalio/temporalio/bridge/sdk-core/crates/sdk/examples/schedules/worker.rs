mod workflows;

use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};
use workflows::{ScheduledActivities, ScheduledWorkflow};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new_assume_tokio(Default::default())?;
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let worker_options = WorkerOptions::new("schedules")
        .register_workflow::<ScheduledWorkflow>()?
        .register_activities(ScheduledActivities)
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    worker.run().await?;

    Ok(())
}

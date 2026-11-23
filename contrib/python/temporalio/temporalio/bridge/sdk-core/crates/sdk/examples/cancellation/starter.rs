mod workflows;

use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowCancelOptions, WorkflowGetResultOptions,
    WorkflowStartOptions, envconfig::LoadClientConfigProfileOptions,
};
use workflows::CancellationWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let handle = client
        .start_workflow(
            CancellationWorkflow::run,
            (),
            WorkflowStartOptions::new("cancellation", "cancellation-workflow-id").build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("Requesting cancellation...");
    handle.cancel(WorkflowCancelOptions::default()).await?;

    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Workflow result: {result}");

    Ok(())
}

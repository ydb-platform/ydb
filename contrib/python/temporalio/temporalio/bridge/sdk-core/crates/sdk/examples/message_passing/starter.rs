mod workflows;

use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowExecuteUpdateOptions, WorkflowGetResultOptions,
    WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};
use workflows::MessagePassingWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let handle = client
        .start_workflow(
            MessagePassingWorkflow::run,
            10,
            WorkflowStartOptions::new("message-passing", "message-passing-workflow-id").build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    handle
        .signal(
            MessagePassingWorkflow::increment,
            5,
            WorkflowSignalOptions::default(),
        )
        .await?;
    println!("Sent signal: increment(5)");

    let counter = handle
        .query(
            MessagePassingWorkflow::get_counter,
            (),
            WorkflowQueryOptions::default(),
        )
        .await?;
    println!("Query result: counter = {counter}");

    let old = handle
        .execute_update(
            MessagePassingWorkflow::set_counter,
            10,
            WorkflowExecuteUpdateOptions::default(),
        )
        .await?;
    println!("Update result: old counter = {old}");

    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Workflow result: {result}");

    Ok(())
}

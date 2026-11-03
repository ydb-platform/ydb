mod workflows;

use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowGetResultOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};
use workflows::LocalActivitiesWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let handle = client
        .start_workflow(
            LocalActivitiesWorkflow::run,
            "Temporal".to_string(),
            WorkflowStartOptions::new("local-activities", "local-activities-workflow-id").build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    let result: (String, String) = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Remote activity result: {}", result.0);
    println!("Local activity result: {}", result.1);

    Ok(())
}

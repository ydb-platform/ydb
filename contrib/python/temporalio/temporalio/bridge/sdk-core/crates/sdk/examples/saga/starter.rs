mod workflows;

use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowGetResultOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};
use workflows::SagaWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let trip_id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "trip-123".to_string());

    let handle = client
        .start_workflow(
            SagaWorkflow::run,
            trip_id.clone(),
            WorkflowStartOptions::new("saga", format!("saga-{trip_id}")).build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    match handle.get_result(WorkflowGetResultOptions::default()).await {
        Ok(bookings) => println!("Bookings: {bookings:?}"),
        Err(e) => println!("Workflow failed (compensations were run): {e}"),
    }

    Ok(())
}

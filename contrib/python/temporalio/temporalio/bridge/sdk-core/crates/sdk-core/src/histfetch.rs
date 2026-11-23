//! Use this binary to fetch histories as proto-encoded binary. The first argument must be a
//! workflow ID. A run id may optionally be provided as the second arg. The history is written to
//! `{workflow_id}_history.bin`.
//!
//! We can use `clap` if this needs more arguments / other stuff later on.

use prost::Message;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, NamespacedClient, WorkflowExecutionInfo,
    WorkflowFetchHistoryOptions,
};
use temporalio_common::protos::temporal::api::history::v1::History;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let copts = ConnectionOptions::new(Url::try_from("http://localhost:7233").unwrap())
        .client_name("histfetch")
        .client_version("0.0")
        .build();
    let connection = Connection::connect(copts).await?;
    let client = Client::new(connection, ClientOptions::new("default").build())?;
    let wf_id = std::env::args()
        .nth(1)
        .expect("must provide workflow id as only argument");
    let run_id = std::env::args().nth(2).filter(|s| !s.is_empty());
    let handle = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: wf_id.clone(),
        run_id,
        first_execution_run_id: None,
    }
    .bind_untyped(client);
    let events = handle
        .fetch_history(WorkflowFetchHistoryOptions::default())
        .await?
        .into_events();
    let hist = History { events };
    // Serialize history to file
    let byteified = hist.encode_to_vec();
    tokio::fs::write(format!("{wf_id}_history.bin"), &byteified).await?;
    Ok(())
}

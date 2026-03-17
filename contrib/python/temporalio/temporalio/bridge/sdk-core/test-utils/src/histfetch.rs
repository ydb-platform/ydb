//! Use this binary to fetch histories as proto-encoded binary. The first argument must be a
//! workflow ID. A run id may optionally be provided as the second arg. The history is written to
//! `{workflow_id}_history.bin`.
//!
//! We can use `clap` if this needs more arguments / other stuff later on.

use prost::Message;
use temporal_client::WorkflowClientTrait;
use temporal_sdk_core_test_utils::get_integ_server_options;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let gw_opts = get_integ_server_options();
    let client = gw_opts.connect("default", None).await?;
    let wf_id = std::env::args()
        .nth(1)
        .expect("must provide workflow id as only argument");
    let run_id = std::env::args().nth(2);
    let hist = client
        .get_workflow_execution_history(wf_id.clone(), run_id, vec![])
        .await?
        .history
        .expect("history field must be populated");
    // Serialize history to file
    let byteified = hist.encode_to_vec();
    tokio::fs::write(format!("{wf_id}_history.bin"), &byteified).await?;
    Ok(())
}

mod workflows;

use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowGetResultOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};
use temporalio_common::search_attributes::SearchAttributes;
use workflows::{INT_FIELD, KEYWORD_FIELD, SearchAttributesWorkflow};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let search_attrs = SearchAttributes::new([
        KEYWORD_FIELD.value_set("initial-value".into()),
        INT_FIELD.value_set(0),
    ]);

    let handle = client
        .start_workflow(
            SearchAttributesWorkflow::run,
            (),
            WorkflowStartOptions::new("search-attributes", "search-attributes-workflow-id")
                .search_attributes(search_attrs)
                .build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Workflow result: {result}");

    Ok(())
}

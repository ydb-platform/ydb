mod workflows;

use futures_util::StreamExt;
use temporalio_client::{
    Client, ClientOptions, Connection,
    envconfig::LoadClientConfigProfileOptions,
    schedules::{
        CreateScheduleOptions, ListSchedulesOptions, ScheduleAction, ScheduleOverlapPolicy,
        ScheduleSpec,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let schedule_id = "demo-schedule";
    let handle = client
        .create_schedule(
            schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    workflows::ScheduledWorkflow::run,
                    "world".to_string(),
                    "schedules",
                    "scheduled-workflow",
                ))
                .spec(ScheduleSpec::from_interval(std::time::Duration::from_secs(
                    10,
                )))
                .trigger_immediately(true)
                .build(),
        )
        .await?;
    println!("Created schedule: {schedule_id}");

    let desc = handle.describe(Default::default()).await?;
    println!("Schedule is paused: {}", desc.paused());

    let mut stream = client.list_schedules(ListSchedulesOptions::default());
    if let Some(entry) = stream.next().await {
        let entry = entry?;
        println!("Found schedule: {:?}", entry);
    }

    handle
        .trigger(ScheduleOverlapPolicy::Unspecified, Default::default())
        .await?;
    println!("Triggered schedule");

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    handle.delete(Default::default()).await?;
    println!("Deleted schedule");

    Ok(())
}

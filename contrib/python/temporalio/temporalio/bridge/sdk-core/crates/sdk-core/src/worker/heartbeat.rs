use crate::{
    WorkerClient, WorkerConfig,
    worker::{
        PollError, PollerBehavior, TaskPollers, Worker, WorkerTelemetry, WorkerVersioningStrategy,
        worker_control_task_queue,
    },
};
use parking_lot::RwLock;
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporalio_client::worker::{
    NamespaceDescriptionSource, SharedNamespaceWorkerTrait, WorkerCallbacks,
};
use temporalio_common::{
    protos::{
        TaskToken,
        coresdk::nexus::{NexusTask, NexusTaskCompletion, nexus_task, nexus_task_completion},
        temporal::api::{
            common::v1::Payload,
            nexus,
            nexusservices::workerservice::v1::{ExecuteCommandsRequest, ExecuteCommandsResponse},
            worker::v1::{
                WorkerCommandResult, worker_command::Type as WorkerCommandType,
                worker_command_result::Type as WorkerCommandResultType,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// SharedNamespaceWorker is responsible for polling nexus-delivered worker commands and sending
/// worker heartbeats to the server. This invokes callbacks on all workers in the same process that
/// share the same namespace.
pub(crate) struct SharedNamespaceWorker {
    callbacks_map: Arc<RwLock<HashMap<Uuid, WorkerCallbacks>>>,
    namespace: String,
    cancel: CancellationToken,
    worker_control_task_queue_enabled: Arc<AtomicBool>,
}

impl SharedNamespaceWorker {
    pub(crate) fn new(
        client: Arc<dyn WorkerClient>,
        namespace: String,
        heartbeat_interval: Duration,
        telemetry: Option<WorkerTelemetry>,
        namespace_description: Arc<NamespaceDescriptionSource>,
    ) -> Result<Self, anyhow::Error> {
        let reset_notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let callbacks_map = Arc::new(RwLock::new(HashMap::<Uuid, WorkerCallbacks>::new()));
        let worker_control_task_queue_enabled = Arc::new(AtomicBool::new(false));

        tokio::spawn({
            let client = client.clone();
            let namespace = namespace.clone();
            let callbacks_map = callbacks_map.clone();
            let cancel = cancel.clone();
            let worker_control_task_queue_enabled = worker_control_task_queue_enabled.clone();
            async move {
                let worker_commands_supported = match namespace_description
                    .resolve(|| client.describe_namespace())
                    .await
                {
                    Ok(namespace_resp) => {
                        let caps = namespace_resp
                            .namespace_info
                            .as_ref()
                            .and_then(|info| info.capabilities.as_ref());
                        if caps.map(|c| c.worker_heartbeats) != Some(true) {
                            debug!(
                                "Worker heartbeating configured for runtime, but server version does not support it."
                            );
                            return;
                        }
                        caps.map(|c| c.worker_commands).unwrap_or(false)
                    }
                    Err(e) => {
                        warn!(error=?e, "Network error while describing namespace for heartbeat capabilities");
                        return;
                    }
                };

                // Returns true if the heartbeat loop should stop.
                async fn tick_heartbeat(
                    client: &Arc<dyn WorkerClient>,
                    namespace: &str,
                    callbacks_map: &Arc<RwLock<HashMap<Uuid, WorkerCallbacks>>>,
                ) -> bool {
                    let hb_callbacks: Vec<_> = callbacks_map
                        .read()
                        .values()
                        .filter_map(|cb| {
                            (cb.heartbeat)()
                                .map(|heartbeat| (heartbeat, cb.heartbeat_success.clone()))
                        })
                        .collect();
                    if hb_callbacks.is_empty() {
                        return false;
                    }
                    let mut hb_to_send = Vec::with_capacity(hb_callbacks.len());
                    for (heartbeat, _) in &hb_callbacks {
                        let mut heartbeat = heartbeat.clone();
                        // All of these heartbeat details rely on a client. To avoid circular
                        // dependencies, this must be populated from within SharedNamespaceWorker
                        // to get info from the current client
                        client.set_heartbeat_client_fields(&mut heartbeat);
                        hb_to_send.push(heartbeat);
                    }
                    if let Err(e) = client
                        .record_worker_heartbeat(namespace.to_owned(), hb_to_send)
                        .await
                    {
                        if matches!(e.code(), tonic::Code::Unimplemented) {
                            return true;
                        }
                        warn!(error=?e, "Network error while sending worker heartbeat");
                    } else {
                        for (_, heartbeat_success) in hb_callbacks {
                            if let Some(heartbeat_success) = heartbeat_success {
                                heartbeat_success();
                            }
                        }
                    }
                    false
                }

                // Returns true if the polling loop should stop.
                async fn handle_nexus_poll(
                    worker: &Worker,
                    callbacks_map: &Arc<RwLock<HashMap<Uuid, WorkerCallbacks>>>,
                    nexus_result: Result<NexusTask, PollError>,
                ) -> bool {
                    match nexus_result {
                        Ok(task) => {
                            handle_worker_command_task(worker, callbacks_map, task).await;
                            false
                        }
                        Err(PollError::ShutDown) => true,
                        Err(e) => {
                            warn!(error=?e, "Error polling nexus task for worker commands");
                            false
                        }
                    }
                }

                if worker_commands_supported {
                    let config = WorkerConfig::builder()
                        .namespace(namespace.clone())
                        .task_queue(worker_control_task_queue(
                            &namespace,
                            &client.worker_grouping_key().to_string(),
                        ))
                        .task_types(WorkerTaskTypes::nexus_only())
                        .max_outstanding_nexus_tasks(5_usize)
                        .versioning_strategy(WorkerVersioningStrategy::None {
                            build_id: "1.0".to_owned(),
                        })
                        .nexus_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
                        .build()
                        .expect("internal shared namespace worker options are valid");
                    match Worker::new_with_pollers(
                        config,
                        None,
                        client.clone(),
                        TaskPollers::Real,
                        telemetry,
                        None,
                        true,
                    ) {
                        Ok(worker) => {
                            worker_control_task_queue_enabled.store(true, Ordering::Relaxed);
                            tokio::spawn({
                                let cm = callbacks_map.clone();
                                let cancel = cancel.clone();
                                let worker_control_task_queue_enabled =
                                    worker_control_task_queue_enabled.clone();
                                async move {
                                    loop {
                                        tokio::select! {
                                            nexus_result = worker.poll_nexus_task() => {
                                                if handle_nexus_poll(&worker, &cm, nexus_result).await {
                                                    break;
                                                }
                                            }
                                            _ = cancel.cancelled() => break,
                                        }
                                    }
                                    worker_control_task_queue_enabled
                                        .store(false, Ordering::Relaxed);
                                    worker.shutdown().await;
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error=?e, "Failed to build worker for nexus command polling");
                        }
                    }
                } else {
                    debug!("Server does not support the worker_commands capability");
                }

                let mut ticker = tokio::time::interval(heartbeat_interval);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            if tick_heartbeat(&client, &namespace, &callbacks_map).await {
                                break;
                            }
                        }
                        _ = reset_notify.notified() => ticker.reset(),
                        _ = cancel.cancelled() => break,
                    }
                }
            }
        });

        Ok(Self {
            callbacks_map,
            namespace,
            cancel,
            worker_control_task_queue_enabled,
        })
    }
}

impl SharedNamespaceWorkerTrait for SharedNamespaceWorker {
    fn namespace(&self) -> String {
        self.namespace.clone()
    }

    fn register_callback(&self, worker_instance_key: Uuid, callbacks: WorkerCallbacks) {
        self.callbacks_map
            .write()
            .insert(worker_instance_key, callbacks);
    }

    fn unregister_callback(&self, worker_instance_key: Uuid) -> (Option<WorkerCallbacks>, bool) {
        let mut callbacks_map = self.callbacks_map.write();
        let callbacks = callbacks_map.remove(&worker_instance_key);
        if callbacks_map.is_empty() {
            self.cancel.cancel();
        }
        (callbacks, callbacks_map.is_empty())
    }

    fn num_workers(&self) -> usize {
        self.callbacks_map.read().len()
    }

    fn worker_control_task_queue_enabled(&self) -> bool {
        self.worker_control_task_queue_enabled
            .load(Ordering::Relaxed)
    }
}

async fn handle_worker_command_task(
    worker: &Worker,
    callbacks_map: &Arc<RwLock<HashMap<Uuid, WorkerCallbacks>>>,
    task: NexusTask,
) {
    let Some(nexus_task::Variant::Task(poll_resp)) = task.variant else {
        return;
    };

    let task_token = poll_resp.task_token.clone();

    let Some(request) = poll_resp.request.as_ref() else {
        warn!("Worker command nexus task missing request");
        return;
    };

    let Some(nexus::v1::request::Variant::StartOperation(start_op)) = &request.variant else {
        warn!("Worker command nexus task has unexpected request variant");
        return;
    };

    let payload_data = start_op
        .payload
        .as_ref()
        .map(|p| p.data.as_slice())
        .unwrap_or_default();

    let exec_req = match ExecuteCommandsRequest::decode(payload_data) {
        Ok(req) => req,
        Err(e) => {
            warn!(error=?e, "Failed to decode ExecuteCommandsRequest");
            return;
        }
    };

    let mut results = Vec::with_capacity(exec_req.commands.len());
    for command in &exec_req.commands {
        let result_type = match &command.r#type {
            Some(WorkerCommandType::CancelActivity(cancel_cmd)) => {
                let tt = TaskToken(cancel_cmd.task_token.clone());
                let cancel_callbacks: Vec<_> = callbacks_map
                    .read()
                    .values()
                    .filter_map(|cb| cb.cancel_activity.clone())
                    .collect();
                for cb in cancel_callbacks {
                    if cb(tt.clone()) {
                        break;
                    }
                }
                Some(WorkerCommandResultType::CancelActivity(
                    temporalio_common::protos::temporal::api::worker::v1::CancelActivityResult {},
                ))
            }
            None => {
                warn!("Worker command has no type set");
                None
            }
        };
        results.push(WorkerCommandResult {
            r#type: result_type,
        });
    }

    let response = ExecuteCommandsResponse { results };
    let response_bytes = response.encode_to_vec();

    let completion = NexusTaskCompletion {
        task_token,
        status: Some(nexus_task_completion::Status::Completed(
            nexus::v1::Response {
                variant: Some(nexus::v1::response::Variant::StartOperation(
                    nexus::v1::StartOperationResponse {
                        variant: Some(nexus::v1::start_operation_response::Variant::SyncSuccess(
                            nexus::v1::start_operation_response::Sync {
                                payload: Some(Payload {
                                    data: response_bytes,
                                    ..Default::default()
                                }),
                                links: vec![],
                            },
                        )),
                    },
                )),
            },
        )),
    };

    if let Err(e) = worker.complete_nexus_task(completion).await {
        warn!(error=?e, "Failed to complete worker command nexus task");
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        WorkerClient,
        test_help::{WorkerExt, test_worker_cfg},
        worker,
        worker::{PollerBehavior, client::mocks::mock_worker_client},
    };
    use prost::Message;
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use temporalio_client::worker::{SharedNamespaceWorkerTrait, WorkerCallbacks};
    use temporalio_common::{
        protos::temporal::api::{
            common::v1::Payload,
            namespace::v1::{NamespaceInfo, namespace_info::Capabilities},
            nexus::v1::{
                Request, StartOperationRequest, request, response, start_operation_response,
            },
            nexusservices::workerservice::v1::{ExecuteCommandsRequest, ExecuteCommandsResponse},
            worker::v1::{
                CancelActivityCommand, EnvironmentInfo, WorkerCommand, WorkerHeartbeat,
                worker_command, worker_command_result,
            },
            workflowservice::v1::{
                DescribeNamespaceResponse, PollNexusTaskQueueResponse,
                RecordWorkerHeartbeatResponse, RespondNexusTaskCompletedResponse,
            },
        },
        worker::WorkerTaskTypes,
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn heartbeat_only_includes_started_workers() {
        let mut mock = mock_worker_client();
        let started_worker_key = Uuid::new_v4();
        let (recorded_tx, recorded_rx) = tokio::sync::oneshot::channel();
        let recorded_tx = Mutex::new(Some(recorded_tx));
        mock.expect_record_worker_heartbeat()
            .times(1)
            .returning(move |_, heartbeats| {
                assert_eq!(heartbeats.len(), 1);
                assert_eq!(
                    heartbeats[0].worker_instance_key,
                    started_worker_key.to_string()
                );
                if let Some(tx) = recorded_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                Ok(RecordWorkerHeartbeatResponse {})
            });
        mock.expect_describe_namespace().returning(|| {
            Ok(DescribeNamespaceResponse {
                namespace_info: Some(NamespaceInfo {
                    capabilities: Some(Capabilities {
                        worker_heartbeats: true,
                        ..Capabilities::default()
                    }),
                    ..NamespaceInfo::default()
                }),
                ..DescribeNamespaceResponse::default()
            })
        });
        let client: Arc<dyn WorkerClient> = Arc::new(mock);
        let shared_worker = super::SharedNamespaceWorker::new(
            client,
            "test-namespace".to_string(),
            Duration::from_millis(100),
            None,
            Arc::new(temporalio_client::worker::NamespaceDescriptionSource::unresolved()),
        )
        .unwrap();
        shared_worker.register_callback(
            Uuid::new_v4(),
            WorkerCallbacks {
                heartbeat: Arc::new(|| None),
                heartbeat_success: None,
                cancel_activity: None,
            },
        );
        shared_worker.register_callback(
            started_worker_key,
            WorkerCallbacks {
                heartbeat: Arc::new(move || {
                    Some(WorkerHeartbeat {
                        worker_instance_key: started_worker_key.to_string(),
                        ..Default::default()
                    })
                }),
                heartbeat_success: None,
                cancel_activity: None,
            },
        );

        tokio::time::timeout(Duration::from_secs(5), recorded_rx)
            .await
            .expect("worker heartbeat was not recorded in time")
            .expect("heartbeat sender was dropped");
        shared_worker.cancel.cancel();
    }

    #[tokio::test]
    async fn worker_heartbeat_basic() {
        let mut mock = mock_worker_client();
        let heartbeat_count = Arc::new(AtomicUsize::new(0));
        let heartbeat_count_clone = heartbeat_count.clone();
        let (third_heartbeat_tx, third_heartbeat_rx) = tokio::sync::oneshot::channel();
        let third_heartbeat_tx = Mutex::new(Some(third_heartbeat_tx));
        mock.expect_poll_workflow_task()
            .returning(move |_namespace, _task_queue| Ok(Default::default()));
        mock.expect_poll_nexus_task()
            .returning(move |_poll_options, _nexus_options| Ok(Default::default()));
        mock.expect_record_worker_heartbeat().times(3).returning(
            move |_namespace, worker_heartbeat| {
                assert_eq!(1, worker_heartbeat.len());
                let heartbeat = worker_heartbeat[0].clone();
                let heartbeat_index = heartbeat_count_clone.fetch_add(1, Ordering::Relaxed);
                if heartbeat_index == 2
                    && let Some(tx) = third_heartbeat_tx.lock().unwrap().take()
                {
                    let _ = tx.send(());
                }
                assert_eq!(heartbeat.environment.is_some(), heartbeat_index < 2);
                let host_info = heartbeat.host_info.clone().unwrap();
                assert_eq!("test-identity", heartbeat.worker_identity);
                assert!(!heartbeat.worker_instance_key.is_empty());
                assert_eq!(
                    host_info.host_name,
                    gethostname::gethostname().to_string_lossy().to_string()
                );
                assert_eq!(host_info.process_id, std::process::id().to_string());
                assert_eq!(heartbeat.sdk_name, "test-core");
                assert_eq!(heartbeat.sdk_version, "0.0.0");
                assert!(heartbeat.heartbeat_time.is_some());
                assert!(heartbeat.start_time.is_some());

                if heartbeat_index == 0 {
                    Err(tonic::Status::unavailable("heartbeat retry"))
                } else {
                    Ok(RecordWorkerHeartbeatResponse {})
                }
            },
        );
        mock.expect_describe_namespace().returning(move || {
            Ok(DescribeNamespaceResponse {
                namespace_info: Some(NamespaceInfo {
                    capabilities: Some(Capabilities {
                        worker_heartbeats: true,
                        ..Capabilities::default()
                    }),
                    ..NamespaceInfo::default()
                }),
                ..DescribeNamespaceResponse::default()
            })
        });

        let config = test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .max_outstanding_activities(1_usize)
            .build()
            .unwrap();

        let client = Arc::new(mock);
        let worker = worker::Worker::new(
            config,
            None,
            client.clone(),
            None,
            Some(Duration::from_millis(100)),
            Some(Arc::new(EnvironmentInfo::default())),
        )
        .unwrap();

        let heartbeat_callback = worker
            .client_worker_registrator
            .heartbeat_manager
            .as_ref()
            .unwrap()
            .heartbeat_callback
            .clone();
        assert!(heartbeat_callback().is_none());
        worker.mark_started();
        assert!(heartbeat_callback().is_some());
        tokio::time::timeout(Duration::from_secs(5), third_heartbeat_rx)
            .await
            .expect("worker heartbeat was not recorded in time")
            .expect("heartbeat sender was dropped");
        worker.drain_activity_poller_and_shutdown().await;

        assert_eq!(3, heartbeat_count.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn heartbeat_reports_auto_enroll_after_worker_starts() {
        let mut mock = mock_worker_client();
        let (reported_tx, reported_rx) = tokio::sync::oneshot::channel();
        let reported_tx = Mutex::new(Some(reported_tx));
        mock.expect_record_worker_heartbeat()
            .returning(move |_, heartbeats| {
                if let Some(tx) = reported_tx.lock().unwrap().take() {
                    let is_autoscaling = heartbeats[0]
                        .activity_poller_info
                        .as_ref()
                        .unwrap()
                        .is_autoscaling;
                    let _ = tx.send(is_autoscaling);
                }
                Ok(RecordWorkerHeartbeatResponse {})
            });
        mock.expect_describe_namespace().times(1).returning(|| {
            Ok(DescribeNamespaceResponse {
                namespace_info: Some(NamespaceInfo {
                    capabilities: Some(Capabilities {
                        worker_heartbeats: true,
                        poller_autoscaling_auto_enroll: true,
                        ..Capabilities::default()
                    }),
                    ..NamespaceInfo::default()
                }),
                ..DescribeNamespaceResponse::default()
            })
        });

        let mut config = test_worker_cfg().build().unwrap();
        config.task_types = WorkerTaskTypes::activity_only();
        let worker = worker::Worker::new(
            config,
            None,
            Arc::new(mock),
            None,
            Some(Duration::from_secs(60)),
            None,
        )
        .unwrap();

        worker.validate().await.unwrap();
        worker.mark_started();
        let reported_autoscaling = tokio::time::timeout(Duration::from_secs(5), reported_rx)
            .await
            .expect("worker heartbeat was not recorded in time")
            .expect("heartbeat sender was dropped");
        worker.drain_activity_poller_and_shutdown().await;

        assert!(
            reported_autoscaling,
            "heartbeat emitted after startup must reflect namespace auto-enrollment"
        );
    }

    #[tokio::test]
    async fn worker_commands_not_polled_when_capability_disabled() {
        let mut mock = mock_worker_client();
        let namespace = format!("{}-{}", crate::test_help::NAMESPACE, Uuid::new_v4());

        let poll_called = Arc::new(AtomicBool::new(false));
        let poll_called_clone = poll_called.clone();
        mock.expect_poll_nexus_task().returning(move |_, _| {
            poll_called_clone.store(true, Ordering::SeqCst);
            Ok(Default::default())
        });

        let heartbeat_count = Arc::new(AtomicUsize::new(0));
        let heartbeat_count_clone = heartbeat_count.clone();
        mock.expect_record_worker_heartbeat()
            .returning(move |_, _| {
                heartbeat_count_clone.fetch_add(1, Ordering::Relaxed);
                Ok(RecordWorkerHeartbeatResponse {})
            });
        mock.expect_describe_namespace().returning(|| {
            Ok(DescribeNamespaceResponse {
                namespace_info: Some(NamespaceInfo {
                    capabilities: Some(Capabilities {
                        worker_heartbeats: true,
                        worker_commands: false,
                        ..Capabilities::default()
                    }),
                    ..NamespaceInfo::default()
                }),
                ..DescribeNamespaceResponse::default()
            })
        });

        let client: Arc<dyn WorkerClient> = Arc::new(mock);
        let shared_worker = super::SharedNamespaceWorker::new(
            client.clone(),
            namespace.clone(),
            Duration::from_millis(100),
            None,
            Arc::new(temporalio_client::worker::NamespaceDescriptionSource::unresolved()),
        )
        .unwrap();

        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
        let callback_tx = Mutex::new(Some(callback_tx));
        shared_worker.register_callback(
            Uuid::new_v4(),
            WorkerCallbacks {
                heartbeat: Arc::new(move || {
                    if let Some(tx) = callback_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    None
                }),
                heartbeat_success: None,
                cancel_activity: None,
            },
        );
        tokio::time::timeout(Duration::from_secs(5), callback_rx)
            .await
            .expect("heartbeat callback was not invoked in time")
            .expect("heartbeat callback sender was dropped");
        tokio::task::yield_now().await;
        assert_eq!(heartbeat_count.load(Ordering::Relaxed), 0);
        assert!(
            !shared_worker
                .worker_control_task_queue_enabled
                .load(Ordering::Relaxed),
            "control task queue must stay disabled without the worker_commands capability"
        );
        shared_worker.cancel.cancel();

        assert!(
            !poll_called.load(Ordering::SeqCst),
            "shared namespace worker must not poll worker commands without the capability"
        );
    }

    #[tokio::test]
    async fn worker_command_cancel_activity() {
        let mut mock = mock_worker_client();
        let namespace = format!("{}-{}", crate::test_help::NAMESPACE, Uuid::new_v4());

        let activity_task_token = vec![1, 2, 3, 4];

        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let completion_tx = Mutex::new(Some(completion_tx));

        let poll_returned_command = Arc::new(AtomicBool::new(false));
        let poll_returned_command_clone = poll_returned_command.clone();
        let at_clone = activity_task_token.clone();
        let expected_task_queue_prefix = format!("temporal-sys/worker-commands/{namespace}/");
        mock.expect_poll_nexus_task()
            .returning(move |poll_options, nexus_options| {
                assert!(
                    nexus_options.worker_commands_queue,
                    "shared namespace worker must poll the worker-commands queue"
                );
                let task_queue_grouping_key = poll_options
                    .task_queue
                    .strip_prefix(&expected_task_queue_prefix)
                    .expect("shared namespace worker must poll its own control task queue");
                Uuid::parse_str(task_queue_grouping_key)
                    .expect("control task queue must include worker grouping key");
                if !poll_returned_command_clone.swap(true, Ordering::SeqCst) {
                    let exec_req = ExecuteCommandsRequest {
                        commands: vec![WorkerCommand {
                            r#type: Some(worker_command::Type::CancelActivity(
                                CancelActivityCommand {
                                    task_token: at_clone.clone(),
                                },
                            )),
                        }],
                    };
                    Ok(PollNexusTaskQueueResponse {
                        task_token: vec![99],
                        request: Some(Request {
                            header: Default::default(),
                            scheduled_time: None,
                            endpoint: String::new(),
                            variant: Some(request::Variant::StartOperation(
                                StartOperationRequest {
                                    service:
                                        "temporal.api.nexusservices.workerservice.v1.WorkerService"
                                            .to_string(),
                                    operation: "ExecuteCommands".to_string(),
                                    request_id: "test-req-id".to_string(),
                                    callback: String::new(),
                                    payload: Some(Payload {
                                        data: exec_req.encode_to_vec(),
                                        ..Default::default()
                                    }),
                                    callback_header: Default::default(),
                                    links: vec![],
                                },
                            )),
                            capabilities: None,
                        }),
                        ..Default::default()
                    })
                } else {
                    Ok(Default::default())
                }
            });
        mock.expect_complete_nexus_task()
            .returning(move |_task_token, response| {
                if let Some(tx) = completion_tx.lock().unwrap().take() {
                    let _ = tx.send(response);
                }
                Ok(RespondNexusTaskCompletedResponse {})
            });

        mock.expect_record_worker_heartbeat()
            .returning(move |_namespace, _worker_heartbeat| Ok(RecordWorkerHeartbeatResponse {}));
        mock.expect_describe_namespace().returning(move || {
            Ok(DescribeNamespaceResponse {
                namespace_info: Some(NamespaceInfo {
                    capabilities: Some(Capabilities {
                        worker_heartbeats: true,
                        worker_commands: true,
                        ..Capabilities::default()
                    }),
                    ..NamespaceInfo::default()
                }),
                ..DescribeNamespaceResponse::default()
            })
        });

        let mut config = test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .max_outstanding_activities(1_usize)
            .build()
            .unwrap();
        config.namespace = namespace.clone();
        config.task_types = WorkerTaskTypes::activity_only();

        let client = Arc::new(mock);
        let worker = worker::Worker::new(
            config,
            None,
            client.clone(),
            None,
            Some(Duration::from_millis(100)),
            None,
        )
        .unwrap();

        worker.mark_started();
        let response = tokio::time::timeout(Duration::from_secs(5), completion_rx)
            .await
            .expect("nexus task was not completed in time")
            .expect("completion sender was dropped");
        assert!(
            client
                .workers()
                .worker_control_task_queue_enabled(&namespace),
            "control task queue must be enabled while the command worker is polling"
        );
        worker.drain_pollers_and_shutdown().await;
        assert!(
            !client
                .workers()
                .worker_control_task_queue_enabled(&namespace),
            "control task queue must be disabled after the command worker shuts down"
        );

        let start_op = match response.variant {
            Some(response::Variant::StartOperation(s)) => s,
            other => panic!("Expected StartOperation response, got {:?}", other),
        };
        let sync_resp = match start_op.variant {
            Some(start_operation_response::Variant::SyncSuccess(s)) => s,
            other => panic!("Expected SyncSuccess, got {:?}", other),
        };
        let payload_data = sync_resp.payload.expect("Should have payload").data;
        let exec_resp = ExecuteCommandsResponse::decode(payload_data.as_slice())
            .expect("Should decode ExecuteCommandsResponse");
        assert_eq!(exec_resp.results.len(), 1, "Should have one result");
        assert!(
            matches!(
                exec_resp.results[0].r#type,
                Some(worker_command_result::Type::CancelActivity(_))
            ),
            "Result should be CancelActivity"
        );
    }
}

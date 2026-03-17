use crate::WorkerClient;
use crate::abstractions::dbg_panic;
use gethostname::gethostname;
use parking_lot::Mutex;
use prost_types::Duration as PbDuration;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use temporal_sdk_core_api::worker::WorkerConfig;
use temporal_sdk_core_protos::temporal::api::worker::v1::{WorkerHeartbeat, WorkerHostInfo};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

pub(crate) type HeartbeatFn = Box<dyn Fn() -> Option<WorkerHeartbeat> + Send + Sync>;

pub(crate) struct WorkerHeartbeatManager {
    heartbeat_handle: JoinHandle<()>,
}

impl WorkerHeartbeatManager {
    pub(crate) fn new(
        config: WorkerConfig,
        identity: String,
        heartbeat_fn: Arc<OnceLock<HeartbeatFn>>,
        client: Arc<dyn WorkerClient>,
    ) -> Self {
        let sdk_name_and_ver = client.sdk_name_and_version();
        let reset_notify = Arc::new(Notify::new());
        let data = Arc::new(Mutex::new(WorkerHeartbeatData::new(
            config,
            identity,
            sdk_name_and_ver,
            reset_notify.clone(),
        )));
        let data_clone = data.clone();

        let heartbeat_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(data_clone.lock().heartbeat_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let heartbeat = if let Some(heartbeat) = data_clone.lock().capture_heartbeat_if_needed() {
                            heartbeat
                        } else {
                            continue
                        };
                        if let Err(e) = client.clone().record_worker_heartbeat(heartbeat).await {
                            if matches!(
                                e.code(),
                                tonic::Code::Unimplemented
                            ) {
                                return;
                            }
                            warn!(error=?e, "Network error while sending worker heartbeat");
                        }
                    }
                    _ = reset_notify.notified() => {
                        ticker.reset();
                    }
                }
            }
        });

        let data_clone = data.clone();
        if heartbeat_fn
            .set(Box::new(move || {
                data_clone.lock().capture_heartbeat_if_needed()
            }))
            .is_err()
        {
            dbg_panic!(
                "Failed to set heartbeat_fn, heartbeat_fn should only be set once, when a singular WorkerHeartbeatInfo is created"
            );
        }

        Self { heartbeat_handle }
    }

    pub(crate) fn shutdown(&self) {
        self.heartbeat_handle.abort()
    }
}

#[derive(Debug, Clone)]
struct WorkerHeartbeatData {
    worker_instance_key: String,
    worker_identity: String,
    host_info: WorkerHostInfo,
    // Time of the last heartbeat. This is used to both for heartbeat_time and last_heartbeat_time
    heartbeat_time: Option<SystemTime>,
    task_queue: String,
    /// SDK name
    sdk_name: String,
    /// SDK version
    sdk_version: String,
    /// Worker start time
    start_time: SystemTime,
    heartbeat_interval: Duration,
    reset_notify: Arc<Notify>,
}

impl WorkerHeartbeatData {
    fn new(
        worker_config: WorkerConfig,
        worker_identity: String,
        sdk_name_and_ver: (String, String),
        reset_notify: Arc<Notify>,
    ) -> Self {
        Self {
            worker_identity,
            host_info: WorkerHostInfo {
                host_name: gethostname().to_string_lossy().to_string(),
                process_id: std::process::id().to_string(),
                ..Default::default()
            },
            sdk_name: sdk_name_and_ver.0,
            sdk_version: sdk_name_and_ver.1,
            task_queue: worker_config.task_queue.clone(),
            start_time: SystemTime::now(),
            heartbeat_time: None,
            worker_instance_key: Uuid::new_v4().to_string(),
            heartbeat_interval: worker_config
                .heartbeat_interval
                .expect("WorkerHeartbeatData is only called when heartbeat_interval is Some"),
            reset_notify,
        }
    }

    fn capture_heartbeat_if_needed(&mut self) -> Option<WorkerHeartbeat> {
        let now = SystemTime::now();
        let elapsed_since_last_heartbeat = if let Some(heartbeat_time) = self.heartbeat_time {
            let dur = now.duration_since(heartbeat_time).unwrap_or(Duration::ZERO);

            // Only send poll data if it's nearly been a full interval since this data has been sent
            // In this case, "nearly" is 90% of the interval
            if dur.as_secs_f64() < 0.9 * self.heartbeat_interval.as_secs_f64() {
                return None;
            }
            Some(PbDuration {
                seconds: dur.as_secs() as i64,
                nanos: dur.subsec_nanos() as i32,
            })
        } else {
            None
        };

        self.heartbeat_time = Some(now);

        self.reset_notify.notify_one();

        Some(WorkerHeartbeat {
            worker_instance_key: self.worker_instance_key.clone(),
            worker_identity: self.worker_identity.clone(),
            host_info: Some(self.host_info.clone()),
            task_queue: self.task_queue.clone(),
            sdk_name: self.sdk_name.clone(),
            sdk_version: self.sdk_version.clone(),
            status: 0,
            start_time: Some(self.start_time.into()),
            heartbeat_time: Some(SystemTime::now().into()),
            elapsed_since_last_heartbeat,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::WorkerExt;
    use crate::test_help::test_worker_cfg;
    use crate::worker;
    use crate::worker::client::mocks::mock_worker_client;
    use std::sync::Arc;
    use std::time::Duration;
    use temporal_sdk_core_api::worker::PollerBehavior;
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::RecordWorkerHeartbeatResponse;

    #[tokio::test]
    async fn worker_heartbeat() {
        let mut mock = mock_worker_client();
        mock.expect_record_worker_heartbeat()
            .times(2)
            .returning(move |heartbeat| {
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
                assert_eq!(heartbeat.task_queue, "q");
                assert!(heartbeat.heartbeat_time.is_some());
                assert!(heartbeat.start_time.is_some());

                Ok(RecordWorkerHeartbeatResponse {})
            });

        let config = test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .max_outstanding_activities(1_usize)
            .heartbeat_interval(Duration::from_millis(200))
            .build()
            .unwrap();

        let heartbeat_fn = Arc::new(OnceLock::new());
        let client = Arc::new(mock);
        let worker = worker::Worker::new(config, None, client, None, Some(heartbeat_fn.clone()));
        heartbeat_fn.get().unwrap()();

        // heartbeat timer fires once
        advance_time(Duration::from_millis(300)).await;
        // it hasn't been >90% of the interval since the last heartbeat, so no data should be returned here
        assert_eq!(None, heartbeat_fn.get().unwrap()());
        // heartbeat timer fires once
        advance_time(Duration::from_millis(300)).await;

        worker.drain_activity_poller_and_shutdown().await;
    }

    async fn advance_time(dur: Duration) {
        tokio::time::pause();
        tokio::time::advance(dur).await;
        tokio::time::resume();
    }
}

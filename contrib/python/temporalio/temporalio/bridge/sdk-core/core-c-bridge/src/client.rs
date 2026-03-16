use crate::{
    ByteArray, ByteArrayRef, CancellationToken, MetadataRef, UserDataHandle, runtime::Runtime,
};

use futures_util::FutureExt;
use prost::bytes::Bytes;
use std::cell::OnceCell;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use temporal_client::{
    ClientKeepAliveConfig, ClientOptions as CoreClientOptions, ClientOptionsBuilder,
    ClientTlsConfig, CloudService, ConfiguredClient, HealthService, HttpConnectProxyOptions,
    OperatorService, RetryClient, RetryConfig, TemporalServiceClientWithMetrics, TestService,
    TlsConfig, WorkflowService, callback_based,
};
use tokio::sync::oneshot;
use tonic::metadata::MetadataKey;
use url::Url;

#[repr(C)]
pub struct ClientOptions {
    pub target_url: ByteArrayRef,
    pub client_name: ByteArrayRef,
    pub client_version: ByteArrayRef,
    pub metadata: MetadataRef,
    pub api_key: ByteArrayRef,
    pub identity: ByteArrayRef,
    pub tls_options: *const ClientTlsOptions,
    pub retry_options: *const ClientRetryOptions,
    pub keep_alive_options: *const ClientKeepAliveOptions,
    pub http_connect_proxy_options: *const ClientHttpConnectProxyOptions,
    /// If this is set, all gRPC calls go through it and no connection is made to server. The client
    /// connection call usually calls this for "GetSystemInfo" before the connect is complete. See
    /// the callback documentation for more important information about usage and data lifetimes.
    ///
    /// When a callback is set, target_url is not used to connect, but it must be set to a valid URL
    /// anyways in case it is used for logging or other reasons. Similarly, other connect-specific
    /// fields like tls_options, keep_alive_options, and http_connect_proxy_options will be
    /// completely ignored if a callback is set.
    pub grpc_override_callback: ClientGrpcOverrideCallback,
    /// Optional user data passed to each callback call.
    pub grpc_override_callback_user_data: *mut libc::c_void,
}

#[repr(C)]
pub struct ClientTlsOptions {
    pub server_root_ca_cert: ByteArrayRef,
    pub domain: ByteArrayRef,
    pub client_cert: ByteArrayRef,
    pub client_private_key: ByteArrayRef,
}

#[repr(C)]
pub struct ClientRetryOptions {
    pub initial_interval_millis: u64,
    pub randomization_factor: f64,
    pub multiplier: f64,
    pub max_interval_millis: u64,
    pub max_elapsed_time_millis: u64,
    pub max_retries: usize,
}

#[repr(C)]
pub struct ClientKeepAliveOptions {
    pub interval_millis: u64,
    pub timeout_millis: u64,
}

#[repr(C)]
pub struct ClientHttpConnectProxyOptions {
    pub target_host: ByteArrayRef,
    pub username: ByteArrayRef,
    pub password: ByteArrayRef,
}

type CoreClient = RetryClient<ConfiguredClient<TemporalServiceClientWithMetrics>>;

pub struct Client {
    pub(crate) runtime: Runtime,
    pub(crate) core: CoreClient,
}

// Expected to outlive all async calls that use it
unsafe impl Send for Client {}
unsafe impl Sync for Client {}

/// If success or fail are not null, they must be manually freed when done.
pub type ClientConnectCallback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    success: *mut Client,
    fail: *const ByteArray,
);

/// Runtime must live as long as client. Options and user data must live through
/// callback.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_connect(
    runtime: *mut Runtime,
    options: *const ClientOptions,
    user_data: *mut libc::c_void,
    callback: ClientConnectCallback,
) {
    let runtime = unsafe { &mut *runtime };
    // Convert opts
    let options = unsafe { &*options };
    let core_options: CoreClientOptions = match options.try_into() {
        Ok(v) => v,
        Err(err) => {
            unsafe {
                callback(
                    user_data,
                    std::ptr::null_mut(),
                    runtime
                        .alloc_utf8(&format!("Invalid options: {err}"))
                        .into_raw(),
                );
            }
            return;
        }
    };
    // Create override if present
    let service_override = options.grpc_override_callback.map(|cb| {
        create_callback_based_grpc_service(runtime, cb, options.grpc_override_callback_user_data)
    });
    // Spawn async call
    let user_data = UserDataHandle(user_data);
    let core = runtime.core.clone();
    runtime.core.tokio_handle().spawn(async move {
        match core_options
            .connect_no_namespace_with_service_override(
                core.telemetry().get_temporal_metric_meter(),
                service_override,
            )
            .await
        {
            Ok(core) => {
                let owned_client = Box::into_raw(Box::new(Client {
                    runtime: runtime.clone(),
                    core,
                }));
                unsafe {
                    callback(user_data.into(), owned_client, std::ptr::null());
                }
            }
            Err(err) => unsafe {
                callback(
                    user_data.into(),
                    std::ptr::null_mut(),
                    runtime
                        .alloc_utf8(&format!("Connection failed: {err}"))
                        .into_raw(),
                );
            },
        }
    });
}

fn create_callback_based_grpc_service(
    runtime: &Runtime,
    cb: unsafe extern "C" fn(request: *mut ClientGrpcOverrideRequest, user_data: *mut libc::c_void),
    user_data: *mut libc::c_void,
) -> callback_based::CallbackBasedGrpcService {
    let runtime = runtime.clone();
    let user_data = Arc::new(UserDataHandle(user_data));
    callback_based::CallbackBasedGrpcService {
        callback: Arc::new(move |req| {
            let runtime = runtime.clone();
            let user_data = user_data.clone();
            async move {
                // Create a oneshot sender/receiver for the result
                let (sender, receiver) = oneshot::channel();

                // Create boxed request that is dropped when the caller sets the response. If the
                // caller does not, this will be a memory leak.
                //
                // We have to cast this to a literal pointer integer because we use spawn_blocking
                // and Rust can't validate things in either of two approaches. The first approach,
                // just moving the *mut to spawn_blocking closure, will not work because it is not
                // send (even if you wrap it in a marked-send struct). The second, approach, moving
                // the box to the closure and into_raw'ing it there won't work because Rust thinks
                // the "req" param to spawn_blocking may outlive this closure even though we're
                // confident in our oneshot use this will never happen.
                let req_ptr = Box::into_raw(Box::new(ClientGrpcOverrideRequest {
                    core: req,
                    built_headers: OnceCell::new(),
                    response_sender: sender,
                })) as usize;

                // We want to make sure it reached user code. If spawn_blocking fails _and_ it
                // didn't reach user code, it is on us to drop the box.
                let reached_user_code = Arc::new(AtomicBool::new(false));

                // Spawn the callback as blocking, failing on join failure. We use spawn_blocking
                // just in case the user is doing something blocking in their closure, but we ask
                // them not to.
                let reached_user_code_clone = reached_user_code.clone();
                let spawn_ret = runtime
                    .core
                    .tokio_handle()
                    .spawn_blocking(move || unsafe {
                        reached_user_code_clone.store(true, Ordering::Relaxed);
                        cb(
                            req_ptr as *mut ClientGrpcOverrideRequest,
                            user_data.clone().0,
                        );
                    })
                    .await;
                if let Err(err) = spawn_ret {
                    // Re-own box so it can be dropped if never reached user code
                    if !reached_user_code.load(Ordering::Relaxed) {
                        let _ = unsafe { Box::from_raw(req_ptr as *mut ClientGrpcOverrideRequest) };
                    }
                    return Err(tonic::Status::internal(format!("{err}")));
                }

                // Wait result and return. The receiver failure in theory can never happen. If it
                // does, it means somehow the sender was dropped, but our code ensures the sender
                // is not dropped until a value is sent. That's why we're panicking here instead
                // of turning this into a Tonic error.
                receiver.await.expect("Unexpected receiver failure")
            }
            .boxed()
        }),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_free(client: *mut Client) {
    unsafe {
        let _ = Box::from_raw(client);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_update_metadata(
    client: *mut Client,
    metadata: ByteArrayRef,
) {
    let client = unsafe { &*client };
    client
        .core
        .get_client()
        .set_headers(metadata.to_string_map_on_newlines());
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_update_api_key(client: *mut Client, api_key: ByteArrayRef) {
    let client = unsafe { &*client };
    client
        .core
        .get_client()
        .set_api_key(api_key.to_option_string());
}

/// Callback that is invoked for every gRPC call if set on the client options.
///
/// Note, temporal_core_client_grpc_override_request_respond is effectively the "free" call for
/// each request. Each request _must_ call that and the request can no longer be valid after that
/// call. However, all of that work and the respond call may be done well after this callback
/// returns. No data lifetime is related to the callback invocation itself.
///
/// Implementers should return as soon as possible and perform the network request in the
/// background.
pub type ClientGrpcOverrideCallback = Option<
    unsafe extern "C" fn(request: *mut ClientGrpcOverrideRequest, user_data: *mut libc::c_void),
>;

/// Representation of gRPC request for the callback.
///
/// Note, temporal_core_client_grpc_override_request_respond is effectively the "free" call for
/// each request. Each request _must_ call that and the request can no longer be valid after that
/// call.
pub struct ClientGrpcOverrideRequest {
    core: callback_based::GrpcRequest,
    built_headers: OnceCell<String>,
    response_sender: oneshot::Sender<Result<callback_based::GrpcSuccessResponse, tonic::Status>>,
}

// Expected to be passed to user thread
unsafe impl Send for ClientGrpcOverrideRequest {}
unsafe impl Sync for ClientGrpcOverrideRequest {}

/// Response provided to temporal_core_client_grpc_override_request_respond. All values referenced
/// inside here must live until that call returns.
#[repr(C)]
pub struct ClientGrpcOverrideResponse {
    /// Numeric gRPC status code, see https://grpc.io/docs/guides/status-codes/. 0 is success, non-0
    /// is failure.
    pub status_code: i32,

    /// Headers for the response if any. Note, this is meant for user-defined metadata/headers, and
    /// not the gRPC system headers (like :status or content-type).
    pub headers: MetadataRef,

    /// Protobuf bytes for a successful response. Ignored if status_code is non-0.
    pub success_proto: ByteArrayRef,

    /// UTF-8 failure message. Ignored if status_code is 0.
    pub fail_message: ByteArrayRef,

    /// Optional details for the gRPC failure. If non-empty, this should be a protobuf-serialized
    /// google.rpc.Status. Ignored if status_code is 0.
    pub fail_details: ByteArrayRef,
}

/// Get a reference to the service name.
///
/// Note, this is only valid until temporal_core_client_grpc_override_request_respond is called.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_grpc_override_request_service(
    req: *const ClientGrpcOverrideRequest,
) -> ByteArrayRef {
    let req = unsafe { &*req };
    req.core.service.as_str().into()
}

/// Get a reference to the RPC name.
///
/// Note, this is only valid until temporal_core_client_grpc_override_request_respond is called.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_grpc_override_request_rpc(
    req: *const ClientGrpcOverrideRequest,
) -> ByteArrayRef {
    let req = unsafe { &*req };
    req.core.rpc.as_str().into()
}

/// Get a reference to the service headers.
///
/// Note, this is only valid until temporal_core_client_grpc_override_request_respond is called.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_grpc_override_request_headers(
    req: *const ClientGrpcOverrideRequest,
) -> MetadataRef {
    let req = unsafe { &*req };
    // Lazily create the headers on first access
    let headers = req.built_headers.get_or_init(|| {
        req.core
            .headers
            .iter()
            .filter_map(|(name, value)| value.to_str().ok().map(|val| (name.as_str(), val)))
            .flat_map(|(k, v)| [k, v])
            .collect::<Vec<_>>()
            .join("\n")
    });
    headers.as_str().into()
}

/// Get a reference to the request protobuf bytes.
///
/// Note, this is only valid until temporal_core_client_grpc_override_request_respond is called.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_grpc_override_request_proto(
    req: *const ClientGrpcOverrideRequest,
) -> ByteArrayRef {
    let req = unsafe { &*req };
    (&*req.core.proto).into()
}

/// Complete the request, freeing all request data.
///
/// The data referenced in the response must live until this function returns. Once this call is
/// made, none of the request data should be considered valid.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_grpc_override_request_respond(
    req: *mut ClientGrpcOverrideRequest,
    resp: ClientGrpcOverrideResponse,
) {
    // This will be dropped at the end of this call
    let req = unsafe { Box::from_raw(req) };
    // Ignore failure if receiver no longer around (e.g. maybe a cancellation)
    let _ = req
        .response_sender
        .send(resp.build_grpc_override_response());
}

impl ClientGrpcOverrideResponse {
    #[allow(clippy::result_large_err)] // Tonic status, even though big, is reasonable as an Err
    fn build_grpc_override_response(
        self,
    ) -> Result<callback_based::GrpcSuccessResponse, tonic::Status> {
        let headers = Self::client_headers_from_metadata_ref(self.headers)
            .map_err(tonic::Status::internal)?;
        if self.status_code == 0 {
            Ok(callback_based::GrpcSuccessResponse {
                headers,
                proto: self.success_proto.to_vec(),
            })
        } else {
            Err(tonic::Status::with_details_and_metadata(
                tonic::Code::from_i32(self.status_code),
                self.fail_message.to_string(),
                Bytes::copy_from_slice(self.fail_details.to_slice()),
                tonic::metadata::MetadataMap::from_headers(headers),
            ))
        }
    }

    fn client_headers_from_metadata_ref(headers: MetadataRef) -> Result<http::HeaderMap, String> {
        let key_values = headers.to_str_map_on_newlines();
        let mut header_map = http::HeaderMap::with_capacity(key_values.len());
        for (k, v) in key_values.into_iter() {
            let name = http::HeaderName::try_from(k)
                .map_err(|e| format!("Invalid header name '{k}': {e}"))?;
            let value = http::HeaderValue::from_str(v)
                .map_err(|e| format!("Invalid header value '{v}': {e}"))?;
            header_map.insert(name, value);
        }
        Ok(header_map)
    }
}

#[repr(C)]
pub struct RpcCallOptions {
    pub service: RpcService,
    pub rpc: ByteArrayRef,
    pub req: ByteArrayRef,
    pub retry: bool,
    pub metadata: MetadataRef,
    /// 0 means no timeout
    pub timeout_millis: u32,
    pub cancellation_token: *const CancellationToken,
}

// Expected to outlive all async calls that use it
unsafe impl Send for RpcCallOptions {}
unsafe impl Sync for RpcCallOptions {}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum RpcService {
    Workflow = 1,
    Operator,
    Cloud,
    Test,
    Health,
}

/// If success or failure byte arrays inside fail are not null, they must be
/// manually freed when done. Either success or failure_message are always
/// present. Status code may still be 0 with a failure message. Failure details
/// represent a protobuf gRPC status message.
pub type ClientRpcCallCallback = unsafe extern "C" fn(
    user_data: *mut libc::c_void,
    success: *const ByteArray,
    status_code: u32,
    failure_message: *const ByteArray,
    failure_details: *const ByteArray,
);

macro_rules! service_call {
    ($service_fn:ident, $client:ident, $options:ident, $cancel_token:ident) => {{
        let call_future = $service_fn(&$client.core, &$options);
        if let Some(cancel_token) = $cancel_token {
            tokio::select! {
                _ = cancel_token.cancelled() => Err(anyhow::anyhow!("Cancelled")),
                v = call_future => v,
            }
        } else {
            call_future.await
        }
    }};
}

/// Client, options, and user data must live through callback.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_client_rpc_call(
    client: *mut Client,
    options: *const RpcCallOptions,
    user_data: *mut libc::c_void,
    callback: ClientRpcCallCallback,
) {
    let client = unsafe { &*client };
    let options = unsafe { &*options };
    let cancel_token = unsafe { options.cancellation_token.as_ref() }.map(|v| v.token.clone());
    let user_data = UserDataHandle(user_data);
    client.runtime.core.tokio_handle().spawn(async move {
        let res = match options.service {
            RpcService::Workflow => {
                service_call!(call_workflow_service, client, options, cancel_token)
            }
            RpcService::Cloud => {
                service_call!(call_cloud_service, client, options, cancel_token)
            }
            RpcService::Operator => {
                service_call!(call_operator_service, client, options, cancel_token)
            }
            RpcService::Test => service_call!(call_test_service, client, options, cancel_token),
            RpcService::Health => service_call!(call_health_service, client, options, cancel_token),
        };
        let (success, status_code, failure_message, failure_details) = match res {
            Ok(b) => (
                ByteArray::from_vec(b).into_raw(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            ),
            Err(err) => match err.downcast::<tonic::Status>() {
                Ok(status) => (
                    std::ptr::null_mut(),
                    status.code() as u32,
                    ByteArray::from_utf8(status.message().to_string()).into_raw(),
                    ByteArray::from_vec(status.details().to_owned()).into_raw(),
                ),
                Err(err) => (
                    std::ptr::null_mut(),
                    0,
                    ByteArray::from_utf8(format!("{err}")).into_raw(),
                    std::ptr::null_mut(),
                ),
            },
        };
        unsafe {
            callback(
                user_data.into(),
                success,
                status_code,
                failure_message,
                failure_details,
            );
        }
    });
}

macro_rules! rpc_call {
    ($client:ident, $call:ident, $call_name:ident) => {
        if $call.retry {
            rpc_resp($client.$call_name(rpc_req($call)?).await)
        } else {
            rpc_resp($client.into_inner().$call_name(rpc_req($call)?).await)
        }
    };
}

macro_rules! rpc_call_on_trait {
    ($client:ident, $call:ident, $trait:tt, $call_name:ident) => {
        if $call.retry {
            rpc_resp($trait::$call_name(&mut $client, rpc_req($call)?).await)
        } else {
            rpc_resp($trait::$call_name(&mut $client.into_inner(), rpc_req($call)?).await)
        }
    };
}

async fn call_workflow_service(
    client: &CoreClient,
    call: &RpcCallOptions,
) -> anyhow::Result<Vec<u8>> {
    let rpc = call.rpc.to_str();
    let mut client = client.clone();
    match rpc {
        "CountWorkflowExecutions" => rpc_call!(client, call, count_workflow_executions),
        "CreateSchedule" => rpc_call!(client, call, create_schedule),
        "CreateWorkflowRule" => rpc_call!(client, call, create_workflow_rule),
        "DeleteSchedule" => rpc_call!(client, call, delete_schedule),
        "DeleteWorkerDeployment" => rpc_call!(client, call, delete_worker_deployment),
        "DeleteWorkerDeploymentVersion" => {
            rpc_call!(client, call, delete_worker_deployment_version)
        }
        "DeleteWorkflowExecution" => rpc_call!(client, call, delete_workflow_execution),
        "DeleteWorkflowRule" => rpc_call!(client, call, delete_workflow_rule),
        "DeprecateNamespace" => rpc_call!(client, call, deprecate_namespace),
        "DescribeBatchOperation" => rpc_call!(client, call, describe_batch_operation),
        "DescribeDeployment" => rpc_call!(client, call, describe_deployment),
        "DescribeNamespace" => rpc_call!(client, call, describe_namespace),
        "DescribeSchedule" => rpc_call!(client, call, describe_schedule),
        "DescribeTaskQueue" => rpc_call!(client, call, describe_task_queue),
        "DescribeWorkerDeployment" => rpc_call!(client, call, describe_worker_deployment),
        "DescribeWorkerDeploymentVersion" => {
            rpc_call!(client, call, describe_worker_deployment_version)
        }
        "DescribeWorkflowExecution" => rpc_call!(client, call, describe_workflow_execution),
        "DescribeWorkflowRule" => rpc_call!(client, call, describe_workflow_rule),
        "ExecuteMultiOperation" => rpc_call!(client, call, execute_multi_operation),
        "FetchWorkerConfig" => rpc_call!(client, call, fetch_worker_config),
        "GetClusterInfo" => rpc_call!(client, call, get_cluster_info),
        "GetCurrentDeployment" => rpc_call!(client, call, get_current_deployment),
        "GetDeploymentReachability" => rpc_call!(client, call, get_deployment_reachability),
        "GetSearchAttributes" => rpc_call!(client, call, get_search_attributes),
        "GetSystemInfo" => rpc_call!(client, call, get_system_info),
        "GetWorkerBuildIdCompatibility" => {
            rpc_call!(client, call, get_worker_build_id_compatibility)
        }
        "GetWorkerTaskReachability" => {
            rpc_call!(client, call, get_worker_task_reachability)
        }
        "GetWorkerVersioningRules" => rpc_call!(client, call, get_worker_versioning_rules),
        "GetWorkflowExecutionHistory" => rpc_call!(client, call, get_workflow_execution_history),
        "GetWorkflowExecutionHistoryReverse" => {
            rpc_call!(client, call, get_workflow_execution_history_reverse)
        }
        "ListArchivedWorkflowExecutions" => {
            rpc_call!(client, call, list_archived_workflow_executions)
        }
        "ListBatchOperations" => rpc_call!(client, call, list_batch_operations),
        "ListClosedWorkflowExecutions" => rpc_call!(client, call, list_closed_workflow_executions),
        "ListDeployments" => rpc_call!(client, call, list_deployments),
        "ListNamespaces" => rpc_call!(client, call, list_namespaces),
        "ListOpenWorkflowExecutions" => rpc_call!(client, call, list_open_workflow_executions),
        "ListScheduleMatchingTimes" => rpc_call!(client, call, list_schedule_matching_times),
        "ListSchedules" => rpc_call!(client, call, list_schedules),
        "ListTaskQueuePartitions" => rpc_call!(client, call, list_task_queue_partitions),
        "ListWorkerDeployments" => rpc_call!(client, call, list_worker_deployments),
        "ListWorkers" => rpc_call!(client, call, list_workers),
        "ListWorkflowExecutions" => rpc_call!(client, call, list_workflow_executions),
        "ListWorkflowRules" => rpc_call!(client, call, list_workflow_rules),
        "PatchSchedule" => rpc_call!(client, call, patch_schedule),
        "PauseActivity" => rpc_call!(client, call, pause_activity),
        "PollActivityTaskQueue" => rpc_call!(client, call, poll_activity_task_queue),
        "PollNexusTaskQueue" => rpc_call!(client, call, poll_nexus_task_queue),
        "PollWorkflowExecutionUpdate" => rpc_call!(client, call, poll_workflow_execution_update),
        "PollWorkflowTaskQueue" => rpc_call!(client, call, poll_workflow_task_queue),
        "QueryWorkflow" => rpc_call!(client, call, query_workflow),
        "RecordActivityTaskHeartbeat" => rpc_call!(client, call, record_activity_task_heartbeat),
        "RecordActivityTaskHeartbeatById" => {
            rpc_call!(client, call, record_activity_task_heartbeat_by_id)
        }
        "RecordWorkerHeartbeat" => rpc_call!(client, call, record_worker_heartbeat),
        "RegisterNamespace" => rpc_call!(client, call, register_namespace),
        "RequestCancelWorkflowExecution" => {
            rpc_call!(client, call, request_cancel_workflow_execution)
        }
        "ResetActivity" => rpc_call!(client, call, reset_activity),
        "ResetStickyTaskQueue" => rpc_call!(client, call, reset_sticky_task_queue),
        "ResetWorkflowExecution" => rpc_call!(client, call, reset_workflow_execution),
        "RespondActivityTaskCanceled" => rpc_call!(client, call, respond_activity_task_canceled),
        "RespondActivityTaskCanceledById" => {
            rpc_call!(client, call, respond_activity_task_canceled_by_id)
        }
        "RespondActivityTaskCompleted" => rpc_call!(client, call, respond_activity_task_completed),
        "RespondActivityTaskCompletedById" => {
            rpc_call!(client, call, respond_activity_task_completed_by_id)
        }
        "RespondActivityTaskFailed" => rpc_call!(client, call, respond_activity_task_failed),
        "RespondActivityTaskFailedById" => {
            rpc_call!(client, call, respond_activity_task_failed_by_id)
        }
        "RespondNexusTaskCompleted" => rpc_call!(client, call, respond_nexus_task_completed),
        "RespondNexusTaskFailed" => rpc_call!(client, call, respond_nexus_task_failed),
        "RespondQueryTaskCompleted" => rpc_call!(client, call, respond_query_task_completed),
        "RespondWorkflowTaskCompleted" => rpc_call!(client, call, respond_workflow_task_completed),
        "RespondWorkflowTaskFailed" => rpc_call!(client, call, respond_workflow_task_failed),
        "ScanWorkflowExecutions" => rpc_call!(client, call, scan_workflow_executions),
        "SetCurrentDeployment" => rpc_call!(client, call, set_current_deployment),
        "SetWorkerDeploymentCurrentVersion" => {
            rpc_call!(client, call, set_worker_deployment_current_version)
        }
        "SetWorkerDeploymentRampingVersion" => {
            rpc_call!(client, call, set_worker_deployment_ramping_version)
        }
        "ShutdownWorker" => rpc_call!(client, call, shutdown_worker),
        "SignalWithStartWorkflowExecution" => {
            rpc_call!(client, call, signal_with_start_workflow_execution)
        }
        "SignalWorkflowExecution" => rpc_call!(client, call, signal_workflow_execution),
        "StartWorkflowExecution" => rpc_call!(client, call, start_workflow_execution),
        "StartBatchOperation" => rpc_call!(client, call, start_batch_operation),
        "StopBatchOperation" => rpc_call!(client, call, stop_batch_operation),
        "TerminateWorkflowExecution" => rpc_call!(client, call, terminate_workflow_execution),
        "TriggerWorkflowRule" => rpc_call!(client, call, trigger_workflow_rule),
        "UnpauseActivity" => {
            rpc_call_on_trait!(client, call, WorkflowService, unpause_activity)
        }
        "UpdateActivityOptions" => {
            rpc_call_on_trait!(client, call, WorkflowService, update_activity_options)
        }
        "UpdateNamespace" => rpc_call_on_trait!(client, call, WorkflowService, update_namespace),
        "UpdateSchedule" => rpc_call!(client, call, update_schedule),
        "UpdateTaskQueueConfig" => rpc_call!(client, call, update_task_queue_config),
        "UpdateWorkerConfig" => rpc_call!(client, call, update_worker_config),
        "UpdateWorkerDeploymentVersionMetadata" => {
            rpc_call!(client, call, update_worker_deployment_version_metadata)
        }
        "UpdateWorkerVersioningRules" => rpc_call!(client, call, update_worker_versioning_rules),
        "UpdateWorkflowExecution" => rpc_call!(client, call, update_workflow_execution),
        "UpdateWorkflowExecutionOptions" => {
            rpc_call!(client, call, update_workflow_execution_options)
        }
        "UpdateWorkerBuildIdCompatibility" => {
            rpc_call!(client, call, update_worker_build_id_compatibility)
        }
        rpc => Err(anyhow::anyhow!("Unknown RPC call {}", rpc)),
    }
}

async fn call_operator_service(
    client: &CoreClient,
    call: &RpcCallOptions,
) -> anyhow::Result<Vec<u8>> {
    let rpc = call.rpc.to_str();
    let mut client = client.clone();
    match rpc {
        "AddOrUpdateRemoteCluster" => rpc_call!(client, call, add_or_update_remote_cluster),
        "AddSearchAttributes" => rpc_call!(client, call, add_search_attributes),
        "CreateNexusEndpoint" => {
            rpc_call_on_trait!(client, call, OperatorService, create_nexus_endpoint)
        }
        "DeleteNamespace" => rpc_call_on_trait!(client, call, OperatorService, delete_namespace),
        "DeleteNexusEndpoint" => {
            rpc_call_on_trait!(client, call, OperatorService, delete_nexus_endpoint)
        }
        "DeleteWorkflowExecution" => rpc_call!(client, call, delete_workflow_execution),
        "GetNexusEndpoint" => rpc_call_on_trait!(client, call, OperatorService, get_nexus_endpoint),
        "ListClusters" => rpc_call!(client, call, list_clusters),
        "ListNexusEndpoints" => rpc_call!(client, call, list_nexus_endpoints),
        "ListSearchAttributes" => rpc_call!(client, call, list_search_attributes),
        "RemoveRemoteCluster" => rpc_call!(client, call, remove_remote_cluster),
        "RemoveSearchAttributes" => rpc_call!(client, call, remove_search_attributes),
        "UpdateNexusEndpoint" => {
            rpc_call_on_trait!(client, call, OperatorService, update_nexus_endpoint)
        }
        rpc => Err(anyhow::anyhow!("Unknown RPC call {}", rpc)),
    }
}

async fn call_cloud_service(client: &CoreClient, call: &RpcCallOptions) -> anyhow::Result<Vec<u8>> {
    let rpc = call.rpc.to_str();
    let mut client = client.clone();
    match rpc {
        "AddNamespaceRegion" => rpc_call!(client, call, add_namespace_region),
        "AddUserGroupMember" => rpc_call!(client, call, add_user_group_member),
        "CreateApiKey" => rpc_call!(client, call, create_api_key),
        "CreateNamespace" => rpc_call!(client, call, create_namespace),
        "CreateNamespaceExportSink" => rpc_call!(client, call, create_namespace_export_sink),
        "CreateNexusEndpoint" => {
            rpc_call_on_trait!(client, call, CloudService, create_nexus_endpoint)
        }
        "CreateServiceAccount" => rpc_call!(client, call, create_service_account),
        "CreateUserGroup" => rpc_call!(client, call, create_user_group),
        "CreateUser" => rpc_call!(client, call, create_user),
        "DeleteApiKey" => rpc_call!(client, call, delete_api_key),
        "DeleteNamespace" => rpc_call_on_trait!(client, call, CloudService, delete_namespace),
        "DeleteNamespaceExportSink" => rpc_call!(client, call, delete_namespace_export_sink),
        "DeleteNamespaceRegion" => rpc_call!(client, call, delete_namespace_region),
        "DeleteNexusEndpoint" => {
            rpc_call_on_trait!(client, call, CloudService, delete_nexus_endpoint)
        }
        "DeleteServiceAccount" => rpc_call!(client, call, delete_service_account),
        "DeleteUserGroup" => rpc_call!(client, call, delete_user_group),
        "DeleteUser" => rpc_call!(client, call, delete_user),
        "FailoverNamespaceRegion" => rpc_call!(client, call, failover_namespace_region),
        "GetAccount" => rpc_call!(client, call, get_account),
        "GetApiKey" => rpc_call!(client, call, get_api_key),
        "GetApiKeys" => rpc_call!(client, call, get_api_keys),
        "GetAsyncOperation" => rpc_call!(client, call, get_async_operation),
        "GetNamespace" => rpc_call!(client, call, get_namespace),
        "GetNamespaceExportSink" => rpc_call!(client, call, get_namespace_export_sink),
        "GetNamespaceExportSinks" => rpc_call!(client, call, get_namespace_export_sinks),
        "GetNamespaces" => rpc_call!(client, call, get_namespaces),
        "GetNexusEndpoint" => rpc_call_on_trait!(client, call, CloudService, get_nexus_endpoint),
        "GetNexusEndpoints" => rpc_call!(client, call, get_nexus_endpoints),
        "GetRegion" => rpc_call!(client, call, get_region),
        "GetRegions" => rpc_call!(client, call, get_regions),
        "GetServiceAccount" => rpc_call!(client, call, get_service_account),
        "GetServiceAccounts" => rpc_call!(client, call, get_service_accounts),
        "GetUsage" => rpc_call!(client, call, get_usage),
        "GetUserGroup" => rpc_call!(client, call, get_user_group),
        "GetUserGroupMembers" => rpc_call!(client, call, get_user_group_members),
        "GetUserGroups" => rpc_call!(client, call, get_user_groups),
        "GetUser" => rpc_call!(client, call, get_user),
        "GetUsers" => rpc_call!(client, call, get_users),
        "RemoveUserGroupMember" => rpc_call!(client, call, remove_user_group_member),
        "RenameCustomSearchAttribute" => rpc_call!(client, call, rename_custom_search_attribute),
        "SetUserGroupNamespaceAccess" => rpc_call!(client, call, set_user_group_namespace_access),
        "SetUserNamespaceAccess" => rpc_call!(client, call, set_user_namespace_access),
        "UpdateAccount" => rpc_call!(client, call, update_account),
        "UpdateApiKey" => rpc_call!(client, call, update_api_key),
        "UpdateNamespace" => rpc_call_on_trait!(client, call, CloudService, update_namespace),
        "UpdateNamespaceExportSink" => rpc_call!(client, call, update_namespace_export_sink),
        "UpdateNexusEndpoint" => {
            rpc_call_on_trait!(client, call, CloudService, update_nexus_endpoint)
        }
        "UpdateServiceAccount" => rpc_call!(client, call, update_service_account),
        "UpdateUserGroup" => rpc_call!(client, call, update_user_group),
        "UpdateUser" => rpc_call!(client, call, update_user),
        "ValidateNamespaceExportSink" => rpc_call!(client, call, validate_namespace_export_sink),
        "UpdateNamespaceTags" => rpc_call!(client, call, update_namespace_tags),
        "CreateConnectivityRule" => rpc_call!(client, call, create_connectivity_rule),
        "GetConnectivityRule" => rpc_call!(client, call, get_connectivity_rule),
        "GetConnectivityRules" => rpc_call!(client, call, get_connectivity_rules),
        "DeleteConnectivityRule" => rpc_call!(client, call, delete_connectivity_rule),
        rpc => Err(anyhow::anyhow!("Unknown RPC call {}", rpc)),
    }
}

async fn call_test_service(client: &CoreClient, call: &RpcCallOptions) -> anyhow::Result<Vec<u8>> {
    let rpc = call.rpc.to_str();
    let mut client = client.clone();
    match rpc {
        "GetCurrentTime" => rpc_call!(client, call, get_current_time),
        "LockTimeSkipping" => rpc_call!(client, call, lock_time_skipping),
        "SleepUntil" => rpc_call!(client, call, sleep_until),
        "Sleep" => rpc_call!(client, call, sleep),
        "UnlockTimeSkippingWithSleep" => rpc_call!(client, call, unlock_time_skipping_with_sleep),
        "UnlockTimeSkipping" => rpc_call!(client, call, unlock_time_skipping),
        rpc => Err(anyhow::anyhow!("Unknown RPC call {}", rpc)),
    }
}

async fn call_health_service(
    client: &CoreClient,
    call: &RpcCallOptions,
) -> anyhow::Result<Vec<u8>> {
    let rpc = call.rpc.to_str();
    let mut client = client.clone();
    match rpc {
        "Check" => rpc_call!(client, call, check),
        "Watch" => Err(anyhow::anyhow!(
            "Health service Watch method is not implemented in C bridge"
        )),
        rpc => Err(anyhow::anyhow!("Unknown RPC call {}", rpc)),
    }
}

fn rpc_req<P: prost::Message + Default>(
    call: &RpcCallOptions,
) -> anyhow::Result<tonic::Request<P>> {
    let proto = P::decode(call.req.to_slice())?;
    let mut req = tonic::Request::new(proto);
    if call.metadata.size > 0 {
        for (k, v) in call.metadata.to_str_map_on_newlines() {
            req.metadata_mut()
                .insert(MetadataKey::from_str(k)?, v.parse()?);
        }
    }
    if call.timeout_millis > 0 {
        req.set_timeout(Duration::from_millis(call.timeout_millis.into()));
    }
    Ok(req)
}

fn rpc_resp<P>(res: Result<tonic::Response<P>, tonic::Status>) -> anyhow::Result<Vec<u8>>
where
    P: prost::Message,
    P: Default,
{
    Ok(res?.get_ref().encode_to_vec())
}

impl TryFrom<&ClientOptions> for CoreClientOptions {
    type Error = anyhow::Error;

    fn try_from(opts: &ClientOptions) -> anyhow::Result<Self> {
        let mut opts_builder = ClientOptionsBuilder::default();
        opts_builder
            .target_url(Url::parse(opts.target_url.to_str())?)
            .client_name(opts.client_name.to_string())
            .client_version(opts.client_version.to_string())
            .identity(opts.identity.to_string())
            .retry_config(
                unsafe { opts.retry_options.as_ref() }.map_or(RetryConfig::default(), |c| c.into()),
            )
            .keep_alive(unsafe { opts.keep_alive_options.as_ref() }.map(Into::into))
            .headers(if opts.metadata.size == 0 {
                None
            } else {
                Some(opts.metadata.to_string_map_on_newlines())
            })
            .api_key(opts.api_key.to_option_string())
            .http_connect_proxy(
                unsafe { opts.http_connect_proxy_options.as_ref() }.map(Into::into),
            );
        if let Some(tls_config) = unsafe { opts.tls_options.as_ref() } {
            opts_builder.tls_cfg(tls_config.try_into()?);
        }
        Ok(opts_builder.build()?)
    }
}

impl TryFrom<&ClientTlsOptions> for TlsConfig {
    type Error = anyhow::Error;

    fn try_from(opts: &ClientTlsOptions) -> anyhow::Result<Self> {
        Ok(TlsConfig {
            server_root_ca_cert: opts.server_root_ca_cert.to_option_vec(),
            domain: opts.domain.to_option_string(),
            client_tls_config: match (
                opts.client_cert.to_option_vec(),
                opts.client_private_key.to_option_vec(),
            ) {
                (None, None) => None,
                (Some(client_cert), Some(client_private_key)) => Some(ClientTlsConfig {
                    client_cert,
                    client_private_key,
                }),
                _ => {
                    return Err(anyhow::anyhow!(
                        "Must have both client cert and private key or neither"
                    ));
                }
            },
        })
    }
}

impl From<&ClientRetryOptions> for RetryConfig {
    fn from(opts: &ClientRetryOptions) -> Self {
        RetryConfig {
            initial_interval: Duration::from_millis(opts.initial_interval_millis),
            randomization_factor: opts.randomization_factor,
            multiplier: opts.multiplier,
            max_interval: Duration::from_millis(opts.max_interval_millis),
            max_elapsed_time: if opts.max_elapsed_time_millis == 0 {
                None
            } else {
                Some(Duration::from_millis(opts.max_elapsed_time_millis))
            },
            max_retries: opts.max_retries,
        }
    }
}

impl From<&ClientKeepAliveOptions> for ClientKeepAliveConfig {
    fn from(opts: &ClientKeepAliveOptions) -> Self {
        ClientKeepAliveConfig {
            interval: Duration::from_millis(opts.interval_millis),
            timeout: Duration::from_millis(opts.timeout_millis),
        }
    }
}

impl From<&ClientHttpConnectProxyOptions> for HttpConnectProxyOptions {
    fn from(opts: &ClientHttpConnectProxyOptions) -> Self {
        HttpConnectProxyOptions {
            target_addr: opts.target_host.to_string(),
            basic_auth: if opts.username.size != 0 && opts.password.size != 0 {
                Some((opts.username.to_string(), opts.password.to_string()))
            } else {
                None
            },
        }
    }
}

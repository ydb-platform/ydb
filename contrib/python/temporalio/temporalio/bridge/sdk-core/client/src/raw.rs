//! We need a way to trait-ify the raw grpc client because there is no other way to get the
//! information we need via interceptors. This module contains the necessary stuff to make that
//! happen.

use crate::{
    Client, ConfiguredClient, InterceptedMetricsSvc, LONG_POLL_TIMEOUT, RequestExt, RetryClient,
    TEMPORAL_NAMESPACE_HEADER_KEY, TemporalServiceClient,
    metrics::{namespace_kv, task_queue_kv},
    raw::sealed::RawClientLike,
    worker_registry::{Slot, SlotManager},
};
use futures_util::{FutureExt, TryFutureExt, future::BoxFuture};
use std::sync::Arc;
use temporal_sdk_core_api::telemetry::metrics::MetricKeyValue;
use temporal_sdk_core_protos::{
    grpc::health::v1::{health_client::HealthClient, *},
    temporal::api::{
        cloud::cloudservice::{v1 as cloudreq, v1::cloud_service_client::CloudServiceClient},
        operatorservice::v1::{operator_service_client::OperatorServiceClient, *},
        taskqueue::v1::TaskQueue,
        testservice::v1::{test_service_client::TestServiceClient, *},
        workflowservice::v1::{workflow_service_client::WorkflowServiceClient, *},
    },
};
use tonic::{
    Request, Response, Status,
    body::Body,
    client::GrpcService,
    metadata::{AsciiMetadataValue, KeyAndValueRef},
};

pub(super) mod sealed {
    use super::*;

    /// Something that has access to the raw grpc services
    #[async_trait::async_trait]
    pub trait RawClientLike: Send {
        type SvcType: Send + Sync + Clone + 'static;

        /// Return a ref to the workflow service client instance
        fn workflow_client(&self) -> &WorkflowServiceClient<Self::SvcType>;

        /// Return a mutable ref to the workflow service client instance
        fn workflow_client_mut(&mut self) -> &mut WorkflowServiceClient<Self::SvcType>;

        /// Return a ref to the operator service client instance
        fn operator_client(&self) -> &OperatorServiceClient<Self::SvcType>;

        /// Return a mutable ref to the operator service client instance
        fn operator_client_mut(&mut self) -> &mut OperatorServiceClient<Self::SvcType>;

        /// Return a ref to the cloud service client instance
        fn cloud_client(&self) -> &CloudServiceClient<Self::SvcType>;

        /// Return a mutable ref to the cloud service client instance
        fn cloud_client_mut(&mut self) -> &mut CloudServiceClient<Self::SvcType>;

        /// Return a ref to the test service client instance
        fn test_client(&self) -> &TestServiceClient<Self::SvcType>;

        /// Return a mutable ref to the test service client instance
        fn test_client_mut(&mut self) -> &mut TestServiceClient<Self::SvcType>;

        /// Return a ref to the health service client instance
        fn health_client(&self) -> &HealthClient<Self::SvcType>;

        /// Return a mutable ref to the health service client instance
        fn health_client_mut(&mut self) -> &mut HealthClient<Self::SvcType>;

        /// Return a registry with workers using this client instance
        fn get_workers_info(&self) -> Option<Arc<SlotManager>>;

        async fn call<F, Req, Resp>(
            &mut self,
            _call_name: &'static str,
            mut callfn: F,
            req: Request<Req>,
        ) -> Result<Response<Resp>, Status>
        where
            Req: Clone + Unpin + Send + Sync + 'static,
            F: FnMut(&mut Self, Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
            F: Send + Sync + Unpin + 'static,
        {
            callfn(self, req).await
        }
    }
}

#[async_trait::async_trait]
impl<RC, T> RawClientLike for RetryClient<RC>
where
    RC: RawClientLike<SvcType = T> + 'static,
    T: Send + Sync + Clone + 'static,
{
    type SvcType = T;

    fn workflow_client(&self) -> &WorkflowServiceClient<Self::SvcType> {
        self.get_client().workflow_client()
    }

    fn workflow_client_mut(&mut self) -> &mut WorkflowServiceClient<Self::SvcType> {
        self.get_client_mut().workflow_client_mut()
    }

    fn operator_client(&self) -> &OperatorServiceClient<Self::SvcType> {
        self.get_client().operator_client()
    }

    fn operator_client_mut(&mut self) -> &mut OperatorServiceClient<Self::SvcType> {
        self.get_client_mut().operator_client_mut()
    }

    fn cloud_client(&self) -> &CloudServiceClient<Self::SvcType> {
        self.get_client().cloud_client()
    }

    fn cloud_client_mut(&mut self) -> &mut CloudServiceClient<Self::SvcType> {
        self.get_client_mut().cloud_client_mut()
    }

    fn test_client(&self) -> &TestServiceClient<Self::SvcType> {
        self.get_client().test_client()
    }

    fn test_client_mut(&mut self) -> &mut TestServiceClient<Self::SvcType> {
        self.get_client_mut().test_client_mut()
    }

    fn health_client(&self) -> &HealthClient<Self::SvcType> {
        self.get_client().health_client()
    }

    fn health_client_mut(&mut self) -> &mut HealthClient<Self::SvcType> {
        self.get_client_mut().health_client_mut()
    }

    fn get_workers_info(&self) -> Option<Arc<SlotManager>> {
        self.get_client().get_workers_info()
    }

    async fn call<F, Req, Resp>(
        &mut self,
        call_name: &'static str,
        mut callfn: F,
        mut req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        F: FnMut(&mut Self, Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        F: Send + Sync + Unpin + 'static,
    {
        let info = self.get_call_info(call_name, Some(&req));
        req.extensions_mut().insert(info.call_type);
        if info.call_type.is_long() {
            req.set_default_timeout(LONG_POLL_TIMEOUT);
        }
        let fact = || {
            let req_clone = req_cloner(&req);
            callfn(self, req_clone)
        };
        let res = Self::make_future_retry(info, fact);
        res.map_err(|(e, _attempt)| e).map_ok(|x| x.0).await
    }
}

impl<T> RawClientLike for TemporalServiceClient<T>
where
    T: Send + Sync + Clone + 'static,
    T: GrpcService<Body> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type SvcType = T;

    fn workflow_client(&self) -> &WorkflowServiceClient<Self::SvcType> {
        self.workflow_svc()
    }

    fn workflow_client_mut(&mut self) -> &mut WorkflowServiceClient<Self::SvcType> {
        self.workflow_svc_mut()
    }

    fn operator_client(&self) -> &OperatorServiceClient<Self::SvcType> {
        self.operator_svc()
    }

    fn operator_client_mut(&mut self) -> &mut OperatorServiceClient<Self::SvcType> {
        self.operator_svc_mut()
    }

    fn cloud_client(&self) -> &CloudServiceClient<Self::SvcType> {
        self.cloud_svc()
    }

    fn cloud_client_mut(&mut self) -> &mut CloudServiceClient<Self::SvcType> {
        self.cloud_svc_mut()
    }

    fn test_client(&self) -> &TestServiceClient<Self::SvcType> {
        self.test_svc()
    }

    fn test_client_mut(&mut self) -> &mut TestServiceClient<Self::SvcType> {
        self.test_svc_mut()
    }

    fn health_client(&self) -> &HealthClient<Self::SvcType> {
        self.health_svc()
    }

    fn health_client_mut(&mut self) -> &mut HealthClient<Self::SvcType> {
        self.health_svc_mut()
    }

    fn get_workers_info(&self) -> Option<Arc<SlotManager>> {
        None
    }
}

impl<T> RawClientLike for ConfiguredClient<TemporalServiceClient<T>>
where
    T: Send + Sync + Clone + 'static,
    T: GrpcService<Body> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type SvcType = T;

    fn workflow_client(&self) -> &WorkflowServiceClient<Self::SvcType> {
        self.client.workflow_client()
    }

    fn workflow_client_mut(&mut self) -> &mut WorkflowServiceClient<Self::SvcType> {
        self.client.workflow_client_mut()
    }

    fn operator_client(&self) -> &OperatorServiceClient<Self::SvcType> {
        self.client.operator_client()
    }

    fn operator_client_mut(&mut self) -> &mut OperatorServiceClient<Self::SvcType> {
        self.client.operator_client_mut()
    }

    fn cloud_client(&self) -> &CloudServiceClient<Self::SvcType> {
        self.client.cloud_client()
    }

    fn cloud_client_mut(&mut self) -> &mut CloudServiceClient<Self::SvcType> {
        self.client.cloud_client_mut()
    }

    fn test_client(&self) -> &TestServiceClient<Self::SvcType> {
        self.client.test_client()
    }

    fn test_client_mut(&mut self) -> &mut TestServiceClient<Self::SvcType> {
        self.client.test_client_mut()
    }

    fn health_client(&self) -> &HealthClient<Self::SvcType> {
        self.client.health_client()
    }

    fn health_client_mut(&mut self) -> &mut HealthClient<Self::SvcType> {
        self.client.health_client_mut()
    }

    fn get_workers_info(&self) -> Option<Arc<SlotManager>> {
        Some(self.workers())
    }
}

impl RawClientLike for Client {
    type SvcType = InterceptedMetricsSvc;

    fn workflow_client(&self) -> &WorkflowServiceClient<Self::SvcType> {
        self.inner.workflow_client()
    }

    fn workflow_client_mut(&mut self) -> &mut WorkflowServiceClient<Self::SvcType> {
        self.inner.workflow_client_mut()
    }

    fn operator_client(&self) -> &OperatorServiceClient<Self::SvcType> {
        self.inner.operator_client()
    }

    fn operator_client_mut(&mut self) -> &mut OperatorServiceClient<Self::SvcType> {
        self.inner.operator_client_mut()
    }

    fn cloud_client(&self) -> &CloudServiceClient<Self::SvcType> {
        self.inner.cloud_client()
    }

    fn cloud_client_mut(&mut self) -> &mut CloudServiceClient<Self::SvcType> {
        self.inner.cloud_client_mut()
    }

    fn test_client(&self) -> &TestServiceClient<Self::SvcType> {
        self.inner.test_client()
    }

    fn test_client_mut(&mut self) -> &mut TestServiceClient<Self::SvcType> {
        self.inner.test_client_mut()
    }

    fn health_client(&self) -> &HealthClient<Self::SvcType> {
        self.inner.health_client()
    }

    fn health_client_mut(&mut self) -> &mut HealthClient<Self::SvcType> {
        self.inner.health_client_mut()
    }

    fn get_workers_info(&self) -> Option<Arc<SlotManager>> {
        self.inner.get_workers_info()
    }
}

/// Helper for cloning a tonic request as long as the inner message may be cloned.
fn req_cloner<T: Clone>(cloneme: &Request<T>) -> Request<T> {
    let msg = cloneme.get_ref().clone();
    let mut new_req = Request::new(msg);
    let new_met = new_req.metadata_mut();
    for kv in cloneme.metadata().iter() {
        match kv {
            KeyAndValueRef::Ascii(k, v) => {
                new_met.insert(k, v.clone());
            }
            KeyAndValueRef::Binary(k, v) => {
                new_met.insert_bin(k, v.clone());
            }
        }
    }
    *new_req.extensions_mut() = cloneme.extensions().clone();
    new_req
}

#[derive(Clone, Debug)]
pub(super) struct AttachMetricLabels {
    pub(super) labels: Vec<MetricKeyValue>,
}
impl AttachMetricLabels {
    pub(super) fn new(kvs: impl Into<Vec<MetricKeyValue>>) -> Self {
        Self { labels: kvs.into() }
    }
    pub(super) fn namespace(ns: impl Into<String>) -> Self {
        AttachMetricLabels::new(vec![namespace_kv(ns.into())])
    }
    pub(super) fn task_q(&mut self, tq: Option<TaskQueue>) -> &mut Self {
        if let Some(tq) = tq {
            self.task_q_str(tq.name);
        }
        self
    }
    pub(super) fn task_q_str(&mut self, tq: impl Into<String>) -> &mut Self {
        self.labels.push(task_queue_kv(tq.into()));
        self
    }
}

/// A request extension that, when set, should make the [RetryClient] consider this call to be a
/// [super::retry::CallType::UserLongPoll]
#[derive(Copy, Clone, Debug)]
pub(super) struct IsUserLongPoll;

// Blanket impl the trait for all raw-client-like things. Since the trait default-implements
// everything, there's nothing to actually implement.
impl<RC, T> WorkflowService for RC
where
    RC: RawClientLike<SvcType = T>,
    T: GrpcService<Body> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
}
impl<RC, T> OperatorService for RC
where
    RC: RawClientLike<SvcType = T>,
    T: GrpcService<Body> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
}
impl<RC, T> CloudService for RC
where
    RC: RawClientLike<SvcType = T>,
    T: GrpcService<Body> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
}
impl<RC, T> TestService for RC
where
    RC: RawClientLike<SvcType = T>,
    T: GrpcService<Body> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
}
impl<RC, T> HealthService for RC
where
    RC: RawClientLike<SvcType = T>,
    T: GrpcService<Body> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
}

/// Helps re-declare gRPC client methods
///
/// There are two forms:
///
/// * The first takes a closure that can modify the request. This is only called once, before the
///   actual rpc call is made, and before determinations are made about the kind of call (long poll
///   or not) and retry policy.
/// * The second takes three closures. The first can modify the request like in the first form.
///   The second can modify the request and return a value, and is called right before every call
///   (including on retries). The third is called with the response to the call after it resolves.
macro_rules! proxy {
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty $(, $closure:expr)?) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            request: impl tonic::IntoRequest<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            #[allow(unused_mut)]
            let mut as_req = request.into_request();
            $( type_closure_arg(&mut as_req, $closure); )*
            #[allow(unused_mut)]
            let fact = |c: &mut Self, mut req: tonic::Request<$req>| {
                let mut c = c.$client_meth().clone();
                async move { c.$method(req).await }.boxed()
            };
            self.call(stringify!($method), fact, as_req)
        }
    };
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty,
     $closure_request:expr, $closure_before:expr, $closure_after:expr) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            request: impl tonic::IntoRequest<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            #[allow(unused_mut)]
            let mut as_req = request.into_request();
            type_closure_arg(&mut as_req, $closure_request);
            #[allow(unused_mut)]
            let fact = |c: &mut Self, mut req: tonic::Request<$req>| {
                let data = type_closure_two_arg(&mut req, c.get_workers_info().unwrap(),
                                                $closure_before);
                let mut c = c.$client_meth().clone();
                async move {
                    type_closure_two_arg(c.$method(req).await, data, $closure_after)
                }.boxed()
            };
            self.call(stringify!($method), fact, as_req)
        }
    };
}
macro_rules! proxier {
    ( $trait_name:ident; $impl_list_name:ident; $client_type:tt; $client_meth:ident;
      $(($method:ident, $req:ty, $resp:ty
         $(, $closure:expr $(, $closure_before:expr, $closure_after:expr)?)? );)* ) => {
        #[cfg(test)]
        const $impl_list_name: &'static [&'static str] = &[$(stringify!($method)),*];
        /// Trait version of the generated client with modifications to attach appropriate metric
        /// labels or whatever else to requests
        pub trait $trait_name: RawClientLike
        where
            // Yo this is wild
            <Self as RawClientLike>::SvcType: GrpcService<Body> + Send + Clone + 'static,
            <<Self as RawClientLike>::SvcType as GrpcService<Body>>::ResponseBody:
                tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
            <<Self as RawClientLike>::SvcType as GrpcService<Body>>::Error:
                Into<tonic::codegen::StdError>,
            <<Self as RawClientLike>::SvcType as GrpcService<Body>>::Future: Send,
            <<<Self as RawClientLike>::SvcType as GrpcService<Body>>::ResponseBody
                as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
        {
            $(
               proxy!($client_type, $client_meth, $method, $req, $resp
                      $(,$closure $(,$closure_before, $closure_after)*)*);
            )*
        }
    };
}

macro_rules! namespaced_request {
    ($req:ident) => {{
        let ns_str = $req.get_ref().namespace.clone();
        // Attach namespace header
        $req.metadata_mut().insert(
            TEMPORAL_NAMESPACE_HEADER_KEY,
            ns_str.parse().unwrap_or_else(|e| {
                warn!("Unable to parse namespace for header: {e:?}");
                AsciiMetadataValue::from_static("")
            }),
        );
        // Init metric labels
        AttachMetricLabels::namespace(ns_str)
    }};
}

// Nice little trick to avoid the callsite asking to type the closure parameter
fn type_closure_arg<T, R>(arg: T, f: impl FnOnce(T) -> R) -> R {
    f(arg)
}

fn type_closure_two_arg<T, R, S>(arg1: R, arg2: T, f: impl FnOnce(R, T) -> S) -> S {
    f(arg1, arg2)
}

proxier! {
    WorkflowService; ALL_IMPLEMENTED_WORKFLOW_SERVICE_RPCS; WorkflowServiceClient; workflow_client_mut;
    (
        register_namespace,
        RegisterNamespaceRequest,
        RegisterNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_namespace,
        DescribeNamespaceRequest,
        DescribeNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_namespaces,
        ListNamespacesRequest,
        ListNamespacesResponse
    );
    (
        update_namespace,
        UpdateNamespaceRequest,
        UpdateNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        deprecate_namespace,
        DeprecateNamespaceRequest,
        DeprecateNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        start_workflow_execution,
        StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        },
        |r, workers| {
            let mut slot: Option<Box<dyn Slot + Send>> = None;
            let req_mut = r.get_mut();
            if req_mut.request_eager_execution {
                let namespace = req_mut.namespace.clone();
                let task_queue = req_mut.task_queue.as_ref()
                                    .map(|tq| tq.name.clone()).unwrap_or_default();
                match workers.try_reserve_wft_slot(namespace, task_queue) {
                    Some(s) => slot = Some(s),
                    None => req_mut.request_eager_execution = false
                }
            }
            slot
        },
        |resp, slot| {
            if let Some(mut s) = slot
                && let Ok(response) = resp.as_ref()
                    && let Some(task) = response.get_ref().clone().eager_workflow_task
                        && let Err(e) = s.schedule_wft(task) {
                            // This is a latency issue, i.e., the client does not need to handle
                            //  this error, because the WFT will be retried after a timeout.
                            warn!(details = ?e, "Eager workflow task rejected by worker.");
                        }
            resp
        }
    );
    (
        get_workflow_execution_history,
        GetWorkflowExecutionHistoryRequest,
        GetWorkflowExecutionHistoryResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
            if r.get_ref().wait_new_event {
                r.extensions_mut().insert(IsUserLongPoll);
            }
        }
    );
    (
        get_workflow_execution_history_reverse,
        GetWorkflowExecutionHistoryReverseRequest,
        GetWorkflowExecutionHistoryReverseResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_workflow_task_queue,
        PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_workflow_task_completed,
        RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_workflow_task_failed,
        RespondWorkflowTaskFailedRequest,
        RespondWorkflowTaskFailedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_activity_task_queue,
        PollActivityTaskQueueRequest,
        PollActivityTaskQueueResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        record_activity_task_heartbeat,
        RecordActivityTaskHeartbeatRequest,
        RecordActivityTaskHeartbeatResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        record_activity_task_heartbeat_by_id,
        RecordActivityTaskHeartbeatByIdRequest,
        RecordActivityTaskHeartbeatByIdResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_completed,
        RespondActivityTaskCompletedRequest,
        RespondActivityTaskCompletedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_completed_by_id,
        RespondActivityTaskCompletedByIdRequest,
        RespondActivityTaskCompletedByIdResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );

    (
        respond_activity_task_failed,
        RespondActivityTaskFailedRequest,
        RespondActivityTaskFailedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_failed_by_id,
        RespondActivityTaskFailedByIdRequest,
        RespondActivityTaskFailedByIdResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_canceled,
        RespondActivityTaskCanceledRequest,
        RespondActivityTaskCanceledResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_canceled_by_id,
        RespondActivityTaskCanceledByIdRequest,
        RespondActivityTaskCanceledByIdResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        request_cancel_workflow_execution,
        RequestCancelWorkflowExecutionRequest,
        RequestCancelWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        signal_workflow_execution,
        SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        signal_with_start_workflow_execution,
        SignalWithStartWorkflowExecutionRequest,
        SignalWithStartWorkflowExecutionResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        reset_workflow_execution,
        ResetWorkflowExecutionRequest,
        ResetWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        terminate_workflow_execution,
        TerminateWorkflowExecutionRequest,
        TerminateWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_workflow_execution,
        DeleteWorkflowExecutionRequest,
        DeleteWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_open_workflow_executions,
        ListOpenWorkflowExecutionsRequest,
        ListOpenWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_closed_workflow_executions,
        ListClosedWorkflowExecutionsRequest,
        ListClosedWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_workflow_executions,
        ListWorkflowExecutionsRequest,
        ListWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_archived_workflow_executions,
        ListArchivedWorkflowExecutionsRequest,
        ListArchivedWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        scan_workflow_executions,
        ScanWorkflowExecutionsRequest,
        ScanWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        count_workflow_executions,
        CountWorkflowExecutionsRequest,
        CountWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        create_workflow_rule,
        CreateWorkflowRuleRequest,
        CreateWorkflowRuleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_workflow_rule,
        DescribeWorkflowRuleRequest,
        DescribeWorkflowRuleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_workflow_rule,
        DeleteWorkflowRuleRequest,
        DeleteWorkflowRuleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_workflow_rules,
        ListWorkflowRulesRequest,
        ListWorkflowRulesResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        trigger_workflow_rule,
        TriggerWorkflowRuleRequest,
        TriggerWorkflowRuleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_search_attributes,
        GetSearchAttributesRequest,
        GetSearchAttributesResponse
    );
    (
        respond_query_task_completed,
        RespondQueryTaskCompletedRequest,
        RespondQueryTaskCompletedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        reset_sticky_task_queue,
        ResetStickyTaskQueueRequest,
        ResetStickyTaskQueueResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        query_workflow,
        QueryWorkflowRequest,
        QueryWorkflowResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_workflow_execution,
        DescribeWorkflowExecutionRequest,
        DescribeWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_task_queue,
        DescribeTaskQueueRequest,
        DescribeTaskQueueResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_cluster_info,
        GetClusterInfoRequest,
        GetClusterInfoResponse
    );
    (
        get_system_info,
        GetSystemInfoRequest,
        GetSystemInfoResponse
    );
    (
        list_task_queue_partitions,
        ListTaskQueuePartitionsRequest,
        ListTaskQueuePartitionsResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        create_schedule,
        CreateScheduleRequest,
        CreateScheduleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_schedule,
        DescribeScheduleRequest,
        DescribeScheduleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_schedule,
        UpdateScheduleRequest,
        UpdateScheduleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        patch_schedule,
        PatchScheduleRequest,
        PatchScheduleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_schedule_matching_times,
        ListScheduleMatchingTimesRequest,
        ListScheduleMatchingTimesResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_schedule,
        DeleteScheduleRequest,
        DeleteScheduleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_schedules,
        ListSchedulesRequest,
        ListSchedulesResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_worker_build_id_compatibility,
        UpdateWorkerBuildIdCompatibilityRequest,
        UpdateWorkerBuildIdCompatibilityResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q_str(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_worker_build_id_compatibility,
        GetWorkerBuildIdCompatibilityRequest,
        GetWorkerBuildIdCompatibilityResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q_str(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_worker_task_reachability,
        GetWorkerTaskReachabilityRequest,
        GetWorkerTaskReachabilityResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_workflow_execution,
        UpdateWorkflowExecutionRequest,
        UpdateWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            let exts = r.extensions_mut();
            exts.insert(labels);
            exts.insert(IsUserLongPoll);
        }
    );
    (
        poll_workflow_execution_update,
        PollWorkflowExecutionUpdateRequest,
        PollWorkflowExecutionUpdateResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        start_batch_operation,
        StartBatchOperationRequest,
        StartBatchOperationResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        stop_batch_operation,
        StopBatchOperationRequest,
        StopBatchOperationResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_batch_operation,
        DescribeBatchOperationRequest,
        DescribeBatchOperationResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_deployment,
        DescribeDeploymentRequest,
        DescribeDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_batch_operations,
        ListBatchOperationsRequest,
        ListBatchOperationsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_deployments,
        ListDeploymentsRequest,
        ListDeploymentsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        execute_multi_operation,
        ExecuteMultiOperationRequest,
        ExecuteMultiOperationResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_current_deployment,
        GetCurrentDeploymentRequest,
        GetCurrentDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_deployment_reachability,
        GetDeploymentReachabilityRequest,
        GetDeploymentReachabilityResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_worker_versioning_rules,
        GetWorkerVersioningRulesRequest,
        GetWorkerVersioningRulesResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q_str(&r.get_ref().task_queue);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_worker_versioning_rules,
        UpdateWorkerVersioningRulesRequest,
        UpdateWorkerVersioningRulesResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q_str(&r.get_ref().task_queue);
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_nexus_task_queue,
        PollNexusTaskQueueRequest,
        PollNexusTaskQueueResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_nexus_task_completed,
        RespondNexusTaskCompletedRequest,
        RespondNexusTaskCompletedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_nexus_task_failed,
        RespondNexusTaskFailedRequest,
        RespondNexusTaskFailedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        set_current_deployment,
        SetCurrentDeploymentRequest,
        SetCurrentDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        shutdown_worker,
        ShutdownWorkerRequest,
        ShutdownWorkerResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_activity_options,
        UpdateActivityOptionsRequest,
        UpdateActivityOptionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        pause_activity,
        PauseActivityRequest,
        PauseActivityResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        unpause_activity,
        UnpauseActivityRequest,
        UnpauseActivityResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_workflow_execution_options,
        UpdateWorkflowExecutionOptionsRequest,
        UpdateWorkflowExecutionOptionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        reset_activity,
        ResetActivityRequest,
        ResetActivityResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_worker_deployment,
        DeleteWorkerDeploymentRequest,
        DeleteWorkerDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_worker_deployment_version,
        DeleteWorkerDeploymentVersionRequest,
        DeleteWorkerDeploymentVersionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_worker_deployment,
        DescribeWorkerDeploymentRequest,
        DescribeWorkerDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_worker_deployment_version,
        DescribeWorkerDeploymentVersionRequest,
        DescribeWorkerDeploymentVersionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_worker_deployments,
        ListWorkerDeploymentsRequest,
        ListWorkerDeploymentsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        set_worker_deployment_current_version,
        SetWorkerDeploymentCurrentVersionRequest,
        SetWorkerDeploymentCurrentVersionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        set_worker_deployment_ramping_version,
        SetWorkerDeploymentRampingVersionRequest,
        SetWorkerDeploymentRampingVersionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_worker_deployment_version_metadata,
        UpdateWorkerDeploymentVersionMetadataRequest,
        UpdateWorkerDeploymentVersionMetadataResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_workers,
        ListWorkersRequest,
        ListWorkersResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        record_worker_heartbeat,
        RecordWorkerHeartbeatRequest,
        RecordWorkerHeartbeatResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_task_queue_config,
        UpdateTaskQueueConfigRequest,
        UpdateTaskQueueConfigResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q_str(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        fetch_worker_config,
        FetchWorkerConfigRequest,
        FetchWorkerConfigResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_worker_config,
        UpdateWorkerConfigRequest,
        UpdateWorkerConfigResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
}

proxier! {
    OperatorService; ALL_IMPLEMENTED_OPERATOR_SERVICE_RPCS; OperatorServiceClient; operator_client_mut;
    (add_search_attributes, AddSearchAttributesRequest, AddSearchAttributesResponse);
    (remove_search_attributes, RemoveSearchAttributesRequest, RemoveSearchAttributesResponse);
    (list_search_attributes, ListSearchAttributesRequest, ListSearchAttributesResponse);
    (delete_namespace, DeleteNamespaceRequest, DeleteNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (add_or_update_remote_cluster, AddOrUpdateRemoteClusterRequest, AddOrUpdateRemoteClusterResponse);
    (remove_remote_cluster, RemoveRemoteClusterRequest, RemoveRemoteClusterResponse);
    (list_clusters, ListClustersRequest, ListClustersResponse);
    (get_nexus_endpoint, GetNexusEndpointRequest, GetNexusEndpointResponse);
    (create_nexus_endpoint, CreateNexusEndpointRequest, CreateNexusEndpointResponse);
    (update_nexus_endpoint, UpdateNexusEndpointRequest, UpdateNexusEndpointResponse);
    (delete_nexus_endpoint, DeleteNexusEndpointRequest, DeleteNexusEndpointResponse);
    (list_nexus_endpoints, ListNexusEndpointsRequest, ListNexusEndpointsResponse);
}

proxier! {
    CloudService; ALL_IMPLEMENTED_CLOUD_SERVICE_RPCS; CloudServiceClient; cloud_client_mut;
    (get_users, cloudreq::GetUsersRequest, cloudreq::GetUsersResponse);
    (get_user, cloudreq::GetUserRequest, cloudreq::GetUserResponse);
    (create_user, cloudreq::CreateUserRequest, cloudreq::CreateUserResponse);
    (update_user, cloudreq::UpdateUserRequest, cloudreq::UpdateUserResponse);
    (delete_user, cloudreq::DeleteUserRequest, cloudreq::DeleteUserResponse);
    (set_user_namespace_access, cloudreq::SetUserNamespaceAccessRequest, cloudreq::SetUserNamespaceAccessResponse);
    (get_async_operation, cloudreq::GetAsyncOperationRequest, cloudreq::GetAsyncOperationResponse);
    (create_namespace, cloudreq::CreateNamespaceRequest, cloudreq::CreateNamespaceResponse);
    (get_namespaces, cloudreq::GetNamespacesRequest, cloudreq::GetNamespacesResponse);
    (get_namespace, cloudreq::GetNamespaceRequest, cloudreq::GetNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (update_namespace, cloudreq::UpdateNamespaceRequest, cloudreq::UpdateNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (rename_custom_search_attribute, cloudreq::RenameCustomSearchAttributeRequest, cloudreq::RenameCustomSearchAttributeResponse);
    (delete_namespace, cloudreq::DeleteNamespaceRequest, cloudreq::DeleteNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (failover_namespace_region, cloudreq::FailoverNamespaceRegionRequest, cloudreq::FailoverNamespaceRegionResponse);
    (add_namespace_region, cloudreq::AddNamespaceRegionRequest, cloudreq::AddNamespaceRegionResponse);
    (delete_namespace_region, cloudreq::DeleteNamespaceRegionRequest, cloudreq::DeleteNamespaceRegionResponse);
    (get_regions, cloudreq::GetRegionsRequest, cloudreq::GetRegionsResponse);
    (get_region, cloudreq::GetRegionRequest, cloudreq::GetRegionResponse);
    (get_api_keys, cloudreq::GetApiKeysRequest, cloudreq::GetApiKeysResponse);
    (get_api_key, cloudreq::GetApiKeyRequest, cloudreq::GetApiKeyResponse);
    (create_api_key, cloudreq::CreateApiKeyRequest, cloudreq::CreateApiKeyResponse);
    (update_api_key, cloudreq::UpdateApiKeyRequest, cloudreq::UpdateApiKeyResponse);
    (delete_api_key, cloudreq::DeleteApiKeyRequest, cloudreq::DeleteApiKeyResponse);
    (get_nexus_endpoints, cloudreq::GetNexusEndpointsRequest, cloudreq::GetNexusEndpointsResponse);
    (get_nexus_endpoint, cloudreq::GetNexusEndpointRequest, cloudreq::GetNexusEndpointResponse);
    (create_nexus_endpoint, cloudreq::CreateNexusEndpointRequest, cloudreq::CreateNexusEndpointResponse);
    (update_nexus_endpoint, cloudreq::UpdateNexusEndpointRequest, cloudreq::UpdateNexusEndpointResponse);
    (delete_nexus_endpoint, cloudreq::DeleteNexusEndpointRequest, cloudreq::DeleteNexusEndpointResponse);
    (get_user_groups, cloudreq::GetUserGroupsRequest, cloudreq::GetUserGroupsResponse);
    (get_user_group, cloudreq::GetUserGroupRequest, cloudreq::GetUserGroupResponse);
    (create_user_group, cloudreq::CreateUserGroupRequest, cloudreq::CreateUserGroupResponse);
    (update_user_group, cloudreq::UpdateUserGroupRequest, cloudreq::UpdateUserGroupResponse);
    (delete_user_group, cloudreq::DeleteUserGroupRequest, cloudreq::DeleteUserGroupResponse);
    (add_user_group_member, cloudreq::AddUserGroupMemberRequest, cloudreq::AddUserGroupMemberResponse);
    (remove_user_group_member, cloudreq::RemoveUserGroupMemberRequest, cloudreq::RemoveUserGroupMemberResponse);
    (get_user_group_members, cloudreq::GetUserGroupMembersRequest, cloudreq::GetUserGroupMembersResponse);
    (set_user_group_namespace_access, cloudreq::SetUserGroupNamespaceAccessRequest, cloudreq::SetUserGroupNamespaceAccessResponse);
    (create_service_account, cloudreq::CreateServiceAccountRequest, cloudreq::CreateServiceAccountResponse);
    (get_service_account, cloudreq::GetServiceAccountRequest, cloudreq::GetServiceAccountResponse);
    (get_service_accounts, cloudreq::GetServiceAccountsRequest, cloudreq::GetServiceAccountsResponse);
    (update_service_account, cloudreq::UpdateServiceAccountRequest, cloudreq::UpdateServiceAccountResponse);
    (delete_service_account, cloudreq::DeleteServiceAccountRequest, cloudreq::DeleteServiceAccountResponse);
    (get_usage, cloudreq::GetUsageRequest, cloudreq::GetUsageResponse);
    (get_account, cloudreq::GetAccountRequest, cloudreq::GetAccountResponse);
    (update_account, cloudreq::UpdateAccountRequest, cloudreq::UpdateAccountResponse);
    (create_namespace_export_sink, cloudreq::CreateNamespaceExportSinkRequest, cloudreq::CreateNamespaceExportSinkResponse);
    (get_namespace_export_sink, cloudreq::GetNamespaceExportSinkRequest, cloudreq::GetNamespaceExportSinkResponse);
    (get_namespace_export_sinks, cloudreq::GetNamespaceExportSinksRequest, cloudreq::GetNamespaceExportSinksResponse);
    (update_namespace_export_sink, cloudreq::UpdateNamespaceExportSinkRequest, cloudreq::UpdateNamespaceExportSinkResponse);
    (delete_namespace_export_sink, cloudreq::DeleteNamespaceExportSinkRequest, cloudreq::DeleteNamespaceExportSinkResponse);
    (validate_namespace_export_sink, cloudreq::ValidateNamespaceExportSinkRequest, cloudreq::ValidateNamespaceExportSinkResponse);
    (update_namespace_tags, cloudreq::UpdateNamespaceTagsRequest, cloudreq::UpdateNamespaceTagsResponse);
    (create_connectivity_rule, cloudreq::CreateConnectivityRuleRequest, cloudreq::CreateConnectivityRuleResponse);
    (get_connectivity_rule, cloudreq::GetConnectivityRuleRequest, cloudreq::GetConnectivityRuleResponse);
    (get_connectivity_rules, cloudreq::GetConnectivityRulesRequest, cloudreq::GetConnectivityRulesResponse);
    (delete_connectivity_rule, cloudreq::DeleteConnectivityRuleRequest, cloudreq::DeleteConnectivityRuleResponse);
}

proxier! {
    TestService; ALL_IMPLEMENTED_TEST_SERVICE_RPCS; TestServiceClient; test_client_mut;
    (lock_time_skipping, LockTimeSkippingRequest, LockTimeSkippingResponse);
    (unlock_time_skipping, UnlockTimeSkippingRequest, UnlockTimeSkippingResponse);
    (sleep, SleepRequest, SleepResponse);
    (sleep_until, SleepUntilRequest, SleepResponse);
    (unlock_time_skipping_with_sleep, SleepRequest, SleepResponse);
    (get_current_time, (), GetCurrentTimeResponse);
}

proxier! {
    HealthService; ALL_IMPLEMENTED_HEALTH_SERVICE_RPCS; HealthClient; health_client_mut;
    (check, HealthCheckRequest, HealthCheckResponse);
    (watch, HealthCheckRequest, tonic::codec::Streaming<HealthCheckResponse>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientOptionsBuilder, RetryClient};
    use std::collections::HashSet;
    use temporal_sdk_core_protos::temporal::api::{
        operatorservice::v1::DeleteNamespaceRequest, workflowservice::v1::ListNamespacesRequest,
    };

    // Just to help make sure some stuff compiles. Not run.
    #[allow(dead_code)]
    async fn raw_client_retry_compiles() {
        let opts = ClientOptionsBuilder::default().build().unwrap();
        let raw_client = opts.connect_no_namespace(None).await.unwrap();
        let mut retry_client = RetryClient::new(raw_client, opts.retry_config);

        let list_ns_req = ListNamespacesRequest::default();
        let fact = |c: &mut RetryClient<_>, req| {
            let mut c = c.workflow_client_mut().clone();
            async move { c.list_namespaces(req).await }.boxed()
        };
        retry_client
            .call("whatever", fact, Request::new(list_ns_req.clone()))
            .await
            .unwrap();

        // Operator svc method
        let op_del_ns_req = DeleteNamespaceRequest::default();
        let fact = |c: &mut RetryClient<_>, req| {
            let mut c = c.operator_client_mut().clone();
            async move { c.delete_namespace(req).await }.boxed()
        };
        retry_client
            .call("whatever", fact, Request::new(op_del_ns_req.clone()))
            .await
            .unwrap();

        // Cloud svc method
        let cloud_del_ns_req = cloudreq::DeleteNamespaceRequest::default();
        let fact = |c: &mut RetryClient<_>, req| {
            let mut c = c.cloud_client_mut().clone();
            async move { c.delete_namespace(req).await }.boxed()
        };
        retry_client
            .call("whatever", fact, Request::new(cloud_del_ns_req.clone()))
            .await
            .unwrap();

        // Verify calling through traits works
        retry_client.list_namespaces(list_ns_req).await.unwrap();
        // Have to disambiguate operator and cloud service
        OperatorService::delete_namespace(&mut retry_client, op_del_ns_req)
            .await
            .unwrap();
        CloudService::delete_namespace(&mut retry_client, cloud_del_ns_req)
            .await
            .unwrap();
        retry_client.get_current_time(()).await.unwrap();
        retry_client
            .check(HealthCheckRequest::default())
            .await
            .unwrap();
    }

    fn verify_methods(proto_def_str: &str, impl_list: &[&str]) {
        let methods: Vec<_> = proto_def_str
            .lines()
            .map(|l| l.trim())
            .filter(|l| l.starts_with("rpc"))
            .map(|l| {
                let stripped = l.strip_prefix("rpc ").unwrap();
                stripped[..stripped.find('(').unwrap()].trim()
            })
            .collect();
        let no_underscores: HashSet<_> = impl_list.iter().map(|x| x.replace('_', "")).collect();
        let mut not_implemented = vec![];
        for method in methods {
            if !no_underscores.contains(&method.to_lowercase()) {
                not_implemented.push(method);
            }
        }
        if !not_implemented.is_empty() {
            panic!(
                "The following RPC methods are not implemented by raw client: {not_implemented:?}"
            );
        }
    }
    #[test]
    fn verify_all_workflow_service_methods_implemented() {
        // This is less work than trying to hook into the codegen process
        let proto_def = include_str!(
            "../../sdk-core-protos/protos/api_upstream/temporal/api/workflowservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_WORKFLOW_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_operator_service_methods_implemented() {
        let proto_def = include_str!(
            "../../sdk-core-protos/protos/api_upstream/temporal/api/operatorservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_OPERATOR_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_cloud_service_methods_implemented() {
        let proto_def = include_str!(
            "../../sdk-core-protos/protos/api_cloud_upstream/temporal/api/cloud/cloudservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_CLOUD_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_test_service_methods_implemented() {
        let proto_def = include_str!(
            "../../sdk-core-protos/protos/testsrv_upstream/temporal/api/testservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_TEST_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_health_service_methods_implemented() {
        let proto_def = include_str!("../../sdk-core-protos/protos/grpc/health/v1/health.proto");
        verify_methods(proto_def, ALL_IMPLEMENTED_HEALTH_SERVICE_RPCS);
    }
}

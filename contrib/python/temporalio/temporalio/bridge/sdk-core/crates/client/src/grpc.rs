//! gRPC service traits for direct access to Temporal services.
//!
//! Most users should use the higher-level methods on [`Client`] or [`Connection`] instead.
//! These traits are useful for advanced scenarios like custom interceptors, testing with mocks,
//! or making raw gRPC calls not covered by the higher-level API.

use crate::{
    Client, Connection, LONG_POLL_TIMEOUT, PayloadErrorLimits, RequestExt, SharedReplaceableClient,
    TEMPORAL_NAMESPACE_HEADER_KEY, TemporalServiceClient,
    metrics::namespace_kv,
    retry::make_future_retry,
    worker::{ClientWorkerSet, Slot},
};
use dyn_clone::DynClone;
use futures_util::{FutureExt, TryFutureExt, future::BoxFuture};
use parking_lot::RwLock;
use std::{any::Any, marker::PhantomData, sync::Arc};
use temporalio_common::{
    payload_limits::{PayloadLimits, validate_known_payload_limits},
    protos::{
        grpc::health::v1::{health_client::HealthClient, *},
        temporal::api::{
            cloud::cloudservice::{v1 as cloudreq, v1::cloud_service_client::CloudServiceClient},
            operatorservice::v1::{operator_service_client::OperatorServiceClient, *},
            taskqueue::v1::TaskQueue,
            testservice::v1::{test_service_client::TestServiceClient, *},
            workflowservice::v1::{workflow_service_client::WorkflowServiceClient, *},
        },
    },
    telemetry::metrics::MetricKeyValue,
};
use tonic::{
    Request, Response, Status,
    body::Body,
    client::GrpcService,
    metadata::{AsciiMetadataValue, KeyAndValueRef},
};

/// Something that has access to the raw grpc services
pub(crate) trait RawClientProducer {
    /// Returns information about workers associated with this client. Implementers outside of
    /// core can safely return `None`.
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>>;

    /// Return a workflow service client instance
    fn workflow_client(&mut self) -> Box<dyn WorkflowService>;

    /// Return a mutable ref to the operator service client instance
    fn operator_client(&mut self) -> Box<dyn OperatorService>;

    /// Return a mutable ref to the cloud service client instance
    fn cloud_client(&mut self) -> Box<dyn CloudService>;

    /// Return a mutable ref to the test service client instance
    fn test_client(&mut self) -> Box<dyn TestService>;

    /// Return a mutable ref to the health service client instance
    fn health_client(&mut self) -> Box<dyn HealthService>;
}

/// Any client that can make gRPC calls. The default implementation simply invokes the passed-in
/// function. Implementers may override this to provide things like retry behavior.
#[async_trait::async_trait]
pub(crate) trait RawGrpcCaller: Send + Sync + 'static {
    /// Make a gRPC call. The default implementation simply invokes the provided function.
    async fn call<F, Req, Resp>(
        &mut self,
        _call_name: &'static str,
        mut callfn: F,
        req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        Resp: Send + 'static,
        F: FnMut(Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        F: Send + Sync + Unpin + 'static,
    {
        callfn(req).await
    }
}

trait ErasedRawClient: Send + Sync + 'static {
    fn erased_call(
        &mut self,
        call_name: &'static str,
        op: &mut dyn ErasedCallOp,
    ) -> BoxFuture<'static, Result<Response<Box<dyn Any + Send>>, Status>>;
}

trait ErasedCallOp: Send {
    fn invoke(
        &mut self,
        raw: &mut dyn ErasedRawClient,
        call_name: &'static str,
    ) -> BoxFuture<'static, Result<Response<Box<dyn Any + Send>>, Status>>;
}

struct CallShim<F, Req, Resp> {
    callfn: F,
    seed_req: Option<Request<Req>>,
    _resp: PhantomData<Resp>,
}

impl<F, Req, Resp> CallShim<F, Req, Resp> {
    fn new(callfn: F, seed_req: Request<Req>) -> Self {
        Self {
            callfn,
            seed_req: Some(seed_req),
            _resp: PhantomData,
        }
    }
}
impl<F, Req, Resp> ErasedCallOp for CallShim<F, Req, Resp>
where
    Req: Clone + Unpin + Send + Sync + 'static,
    Resp: Send + 'static,
    F: FnMut(Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
    F: Send + Sync + Unpin + 'static,
{
    fn invoke(
        &mut self,
        _raw: &mut dyn ErasedRawClient,
        _call_name: &'static str,
    ) -> BoxFuture<'static, Result<Response<Box<dyn Any + Send>>, Status>> {
        (self.callfn)(
            self.seed_req
                .take()
                .expect("CallShim must have request populated"),
        )
        .map(|res| res.map(|payload| payload.map(|t| Box::new(t) as Box<dyn Any + Send>)))
        .boxed()
    }
}

#[async_trait::async_trait]
impl RawGrpcCaller for Connection {
    async fn call<F, Req, Resp>(
        &mut self,
        call_name: &'static str,
        mut callfn: F,
        mut req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        Resp: Send + 'static,
        F: FnMut(Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        F: Send + Sync + Unpin + 'static,
    {
        // Validate payload sizes after any request mutation but before encoding/metrics.
        validate_request_payload_limits(
            &req,
            self.inner.payloads_warn_size,
            self.inner.memo_warn_size,
        )?;

        let info = self
            .inner
            .retry_options
            .get_call_info(call_name, Some(&req));
        req.extensions_mut().insert(info.call_type);
        if info.call_type.is_long() {
            req.set_default_timeout(LONG_POLL_TIMEOUT);
        }

        let fact = || {
            let req_clone = req_cloner(&req);
            callfn(req_clone)
        };

        let res = make_future_retry(info, fact);
        res.map_err(|(e, _attempt)| e).map_ok(|x| x.0).await
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

/// `*_warn` are the connection's configured warn thresholds; per-call error limits ride a
/// [`PayloadErrorLimits`] extension. On an error-level violation, returns a [`Status`] carrying
/// the [`PayloadLimitViolation`] as its source (extract via [crate::payload_limit_violation_from]).
fn validate_request_payload_limits<Req: Any>(
    req: &Request<Req>,
    blob_warn: usize,
    memo_warn: usize,
) -> Result<(), Status> {
    let mut limits = PayloadLimits {
        blob_warn,
        memo_warn,
        blob_error: 0,
        memo_error: 0,
    };
    if let Some(error_limits) = req.extensions().get::<PayloadErrorLimits>() {
        limits.blob_error = error_limits.blob;
        limits.memo_error = error_limits.memo;
    }
    if let Some(violation) = validate_known_payload_limits(req.get_ref(), &limits) {
        let mut status = Status::invalid_argument(violation.to_string());
        status.set_source(Arc::new(violation));
        return Err(status);
    }
    Ok(())
}

#[async_trait::async_trait]
impl RawGrpcCaller for dyn ErasedRawClient {
    async fn call<F, Req, Resp>(
        &mut self,
        call_name: &'static str,
        callfn: F,
        req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        Resp: Send + 'static,
        F: FnMut(Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        F: Send + Sync + Unpin + 'static,
    {
        let mut shim = CallShim::new(callfn, req);
        let erased_resp = ErasedRawClient::erased_call(self, call_name, &mut shim).await?;
        Ok(erased_resp.map(|boxed| {
            *boxed
                .downcast()
                .expect("RawGrpcCaller erased response type mismatch")
        }))
    }
}

impl<T> ErasedRawClient for T
where
    T: RawGrpcCaller + 'static,
{
    fn erased_call(
        &mut self,
        call_name: &'static str,
        op: &mut dyn ErasedCallOp,
    ) -> BoxFuture<'static, Result<Response<Box<dyn Any + Send>>, Status>> {
        let raw: &mut dyn ErasedRawClient = self;
        op.invoke(raw, call_name)
    }
}

impl RawClientProducer for Connection {
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
        Some(self.workers())
    }

    fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
        self.inner.service.workflow_service()
    }

    fn operator_client(&mut self) -> Box<dyn OperatorService> {
        self.inner.service.operator_service()
    }

    fn cloud_client(&mut self) -> Box<dyn CloudService> {
        self.inner.service.cloud_service()
    }

    fn test_client(&mut self) -> Box<dyn TestService> {
        self.inner.service.test_service()
    }

    fn health_client(&mut self) -> Box<dyn HealthService> {
        self.inner.service.health_service()
    }
}

impl RawClientProducer for TemporalServiceClient {
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
        None
    }

    fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
        self.workflow_service()
    }

    fn operator_client(&mut self) -> Box<dyn OperatorService> {
        self.operator_service()
    }

    fn cloud_client(&mut self) -> Box<dyn CloudService> {
        self.cloud_service()
    }

    fn test_client(&mut self) -> Box<dyn TestService> {
        self.test_service()
    }

    fn health_client(&mut self) -> Box<dyn HealthService> {
        self.health_service()
    }
}

impl RawGrpcCaller for TemporalServiceClient {}

impl RawClientProducer for Client {
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
        Some(self.connection.workers())
    }

    fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
        self.connection.workflow_client()
    }

    fn operator_client(&mut self) -> Box<dyn OperatorService> {
        self.connection.operator_client()
    }

    fn cloud_client(&mut self) -> Box<dyn CloudService> {
        self.connection.cloud_client()
    }

    fn test_client(&mut self) -> Box<dyn TestService> {
        self.connection.test_client()
    }

    fn health_client(&mut self) -> Box<dyn HealthService> {
        self.connection.health_client()
    }
}

#[async_trait::async_trait]
impl RawGrpcCaller for Client {
    async fn call<F, Req, Resp>(
        &mut self,
        call_name: &'static str,
        callfn: F,
        req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        Resp: Send + 'static,
        F: FnMut(Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        F: Send + Sync + Unpin + 'static,
    {
        self.connection.call(call_name, callfn, req).await
    }
}

impl<RC> RawClientProducer for SharedReplaceableClient<RC>
where
    RC: RawClientProducer + Clone + Send + Sync + 'static,
{
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
        self.inner_cow().get_workers_info()
    }
    fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
        self.inner_mut_refreshed().workflow_client()
    }

    fn operator_client(&mut self) -> Box<dyn OperatorService> {
        self.inner_mut_refreshed().operator_client()
    }

    fn cloud_client(&mut self) -> Box<dyn CloudService> {
        self.inner_mut_refreshed().cloud_client()
    }

    fn test_client(&mut self) -> Box<dyn TestService> {
        self.inner_mut_refreshed().test_client()
    }

    fn health_client(&mut self) -> Box<dyn HealthService> {
        self.inner_mut_refreshed().health_client()
    }
}

#[async_trait::async_trait]
impl<RC> RawGrpcCaller for SharedReplaceableClient<RC>
where
    RC: RawGrpcCaller + Clone + Sync + 'static,
{
    async fn call<F, Req, Resp>(
        &mut self,
        call_name: &'static str,
        callfn: F,
        req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        Resp: Send + 'static,
        F: FnMut(Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        F: Send + Sync + Unpin + 'static,
    {
        self.inner_mut_refreshed()
            .call(call_name, callfn, req)
            .await
    }
}

/// Wraps a client and injects a worker's configured [`PayloadErrorLimits`] (when set) as a request
/// extension on every gRPC call, so the gRPC layer enforces error limits uniformly across all
/// outbound requests — current and future — without each call site having to opt in.
///
/// The limits are shared and may be updated after construction (e.g. once a worker learns the
/// namespace limits) via [`set_error_limits`](Self::set_error_limits); clones observe the update.
#[doc(hidden)]
#[derive(Clone)]
pub struct PayloadLimitsClient<C> {
    inner: C,
    error_limits: Arc<RwLock<Option<PayloadErrorLimits>>>,
}

impl<C> PayloadLimitsClient<C> {
    /// Wrap `inner`; no limits are enforced until [`set_error_limits`](Self::set_error_limits).
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            error_limits: Arc::new(RwLock::new(None)),
        }
    }

    /// Set (or clear) the error limits injected on every call. Shared across clones.
    pub fn set_error_limits(&self, limits: Option<PayloadErrorLimits>) {
        *self.error_limits.write() = limits;
    }

    /// Return the error limits currently injected into requests.
    pub fn error_limits(&self) -> Option<PayloadErrorLimits> {
        *self.error_limits.read()
    }
}

impl<C: RawClientProducer> RawClientProducer for PayloadLimitsClient<C> {
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
        self.inner.get_workers_info()
    }
    fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
        self.inner.workflow_client()
    }
    fn operator_client(&mut self) -> Box<dyn OperatorService> {
        self.inner.operator_client()
    }
    fn cloud_client(&mut self) -> Box<dyn CloudService> {
        self.inner.cloud_client()
    }
    fn test_client(&mut self) -> Box<dyn TestService> {
        self.inner.test_client()
    }
    fn health_client(&mut self) -> Box<dyn HealthService> {
        self.inner.health_client()
    }
}

#[async_trait::async_trait]
impl<C> RawGrpcCaller for PayloadLimitsClient<C>
where
    C: RawGrpcCaller + Clone + Sync + 'static,
{
    async fn call<F, Req, Resp>(
        &mut self,
        call_name: &'static str,
        callfn: F,
        mut req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        Resp: Send + 'static,
        F: FnMut(Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        F: Send + Sync + Unpin + 'static,
    {
        let limits = *self.error_limits.read();
        if let Some(limits) = limits {
            req.extensions_mut().insert(limits);
        }
        self.inner.call(call_name, callfn, req).await
    }
}

#[derive(Clone, Debug)]
pub(super) struct AttachMetricLabels {
    pub(super) labels: Vec<MetricKeyValue>,
    pub(super) normal_task_queue: Option<String>,
    pub(super) sticky_task_queue: Option<String>,
}
impl AttachMetricLabels {
    pub(super) fn new(kvs: impl Into<Vec<MetricKeyValue>>) -> Self {
        Self {
            labels: kvs.into(),
            normal_task_queue: None,
            sticky_task_queue: None,
        }
    }
    pub(super) fn namespace(ns: impl Into<String>) -> Self {
        AttachMetricLabels::new(vec![namespace_kv(ns.into())])
    }
    pub(super) fn task_q(&mut self, tq: Option<TaskQueue>) -> &mut Self {
        if let Some(tq) = tq {
            if !tq.normal_name.is_empty() {
                self.sticky_task_queue = Some(tq.name);
                self.normal_task_queue = Some(tq.normal_name);
            } else {
                self.normal_task_queue = Some(tq.name);
            }
        }
        self
    }
    pub(super) fn task_q_str(&mut self, tq: impl Into<String>) -> &mut Self {
        self.normal_task_queue = Some(tq.into());
        self
    }
}

/// A request extension that, when set, should make the [RetryClient] consider this call to be a
/// [super::retry::CallType::UserLongPoll]
#[derive(Copy, Clone, Debug)]
pub(super) struct IsUserLongPoll;

macro_rules! proxy_def {
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty, defaults) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            _request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            async { Ok(tonic::Response::new(<$resp>::default())) }.boxed()
        }
    };
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            _request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>>;
    };
}

/// Helps re-declare gRPC client methods
///
/// There are four forms:
///
/// * The first takes a closure that can modify the request. This is only called once, before the
///   actual rpc call is made, and before determinations are made about the kind of call (long poll
///   or not) and retry policy.
/// * The second takes three closures. The first can modify the request like in the first form.
///   The second can modify the request and return a value, and is called right before every call
///   (including on retries). The third is called with the response to the call after it resolves.
/// * The third and fourth are equivalents of the above that skip calling through the `call` method
///   and are implemented directly on the generated gRPC clients (IE: the bottom of the stack).
macro_rules! proxy_impl {
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty $(, $closure:expr)?) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            #[allow(unused_mut)]
            mut request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            $( type_closure_arg(&mut request, $closure); )*
            let mut self_clone = self.clone();
            #[allow(unused_mut)]
            let fact = move |mut req: tonic::Request<$req>| {
                let mut c = self_clone.$client_meth();
                async move { c.$method(req).await }.boxed()
            };
            self.call(stringify!($method), fact, request)
        }
    };
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty,
     $closure_request:expr, $closure_before:expr, $closure_after:expr) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            mut request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            type_closure_arg(&mut request, $closure_request);
            let workers_info = self.get_workers_info();
            let mut self_clone = self.clone();
            #[allow(unused_mut)]
            let fact = move |mut req: tonic::Request<$req>| {
                let data = type_closure_two_arg(&mut req, workers_info.clone(), $closure_before);
                let mut c = self_clone.$client_meth();
                async move {
                    type_closure_two_arg(c.$method(req).await, data, $closure_after)
                }.boxed()
            };
            self.call(stringify!($method), fact, request)
        }
    };
    ($client_type:tt, $method:ident, $req:ty, $resp:ty $(, $closure:expr)?) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            #[allow(unused_mut)]
            mut request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            $( type_closure_arg(&mut request, $closure); )*
            async move { <$client_type<_>>::$method(self, request).await }.boxed()
        }
    };
    ($client_type:tt, $method:ident, $req:ty, $resp:ty,
     $closure_request:expr, $closure_before:expr, $closure_after:expr) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            mut request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            type_closure_arg(&mut request, $closure_request);
            let data = type_closure_two_arg(&mut request, Option::<Arc<ClientWorkerSet>>::None,
                                            $closure_before);
            async move {
                type_closure_two_arg(<$client_type<_>>::$method(self, request).await,
                                     data, $closure_after)
            }.boxed()
        }
    };
}
macro_rules! proxier_impl {
    ($trait_name:ident; $impl_list_name:ident; $client_type:tt; $client_meth:ident;
     [$( proxy_def!($($def_args:tt)*); )*];
     $(($method:ident, $req:ty, $resp:ty
       $(, $closure:expr $(, $closure_before:expr, $closure_after:expr)?)? );)* ) => {
        #[cfg(test)]
        const $impl_list_name: &'static [&'static str] = &[$(stringify!($method)),*];

        #[doc = concat!("Trait version of [", stringify!($client_type), "]")]
        pub trait $trait_name: Send + Sync + DynClone
        {
            $( proxy_def!($($def_args)*); )*
        }
        dyn_clone::clone_trait_object!($trait_name);

        // `allow(deprecated)`: upstream proto deprecations (e.g. cloud-api
        // AddNamespaceRegion) generate calls to deprecated tonic client
        // methods inside the impls; we still wire them for back-compat.
        #[allow(deprecated)]
        impl<RC> $trait_name for RC
        where
            RC: RawGrpcCaller + RawClientProducer + Clone + Unpin,
        {
            $(
                proxy_impl!($client_type, $client_meth, $method, $req, $resp
                            $(,$closure $(,$closure_before, $closure_after)*)*);
            )*
        }

        impl<T: Send + Sync + 'static> RawGrpcCaller for $client_type<T> {}

        #[allow(deprecated)]
        impl<T> $trait_name for $client_type<T>
        where
            T: GrpcService<Body> + Clone + Send + Sync + 'static,
            T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
            T::Error: Into<tonic::codegen::StdError>,
            <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
            <T as tonic::client::GrpcService<Body>>::Future: Send
        {
            $(
                proxy_impl!($client_type, $method, $req, $resp
                            $(,$closure $(,$closure_before, $closure_after)*)*);
            )*
        }
    };
}

macro_rules! proxier {
    ( $trait_name:ident; $impl_list_name:ident; $client_type:tt; $client_meth:ident;
      $(($method:ident, $req:ty, $resp:ty
         $(, $closure:expr $(, $closure_before:expr, $closure_after:expr)?)? );)* ) => {
        proxier_impl!($trait_name; $impl_list_name; $client_type; $client_meth;
                      [$(proxy_def!($client_type, $client_meth, $method, $req, $resp);)*];
                      $(($method, $req, $resp $(, $closure $(, $closure_before, $closure_after)?)?);)*);
    };
    ( $trait_name:ident; $impl_list_name:ident; $client_type:tt; $client_meth:ident; defaults;
      $(($method:ident, $req:ty, $resp:ty
         $(, $closure:expr $(, $closure_before:expr, $closure_after:expr)?)? );)* ) => {
        proxier_impl!($trait_name; $impl_list_name; $client_type; $client_meth;
                      [$(proxy_def!($client_type, $client_meth, $method, $req, $resp, defaults);)*];
                      $(($method, $req, $resp $(, $closure $(, $closure_before, $closure_after)?)?);)*);
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
    WorkflowService; ALL_IMPLEMENTED_WORKFLOW_SERVICE_RPCS; WorkflowServiceClient; workflow_client; defaults;
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
            if let Some(workers) = workers {
                let mut slot: Option<Box<dyn Slot + Send>> = None;
                let req_mut = r.get_mut();
                if req_mut.request_eager_execution {
                    let namespace = req_mut.namespace.clone();
                    let task_queue = req_mut.task_queue.as_ref()
                                        .map(|tq| tq.name.clone()).unwrap_or_default();
                    match workers.try_reserve_wft_slot(namespace, task_queue) {
                        Some(reservation) => {
                            // Populate eager_worker_deployment_options from the slot reservation
                            if let Some(opts) = reservation.deployment_options {
                                req_mut.eager_worker_deployment_options = Some(temporalio_common::protos::temporal::api::deployment::v1::WorkerDeploymentOptions {
                                    deployment_name: opts.version.deployment_name,
                                    build_id: opts.version.build_id,
                                    worker_versioning_mode: if opts.use_worker_versioning {
                                        temporalio_common::protos::temporal::api::enums::v1::WorkerVersioningMode::Versioned.into()
                                    } else {
                                        temporalio_common::protos::temporal::api::enums::v1::WorkerVersioningMode::Unversioned.into()
                                    },
                                });                            }
                            slot = Some(reservation.slot);
                        }
                        None => req_mut.request_eager_execution = false
                    }
                }
                slot
            } else {
                None
            }
        },
        |resp, slot| {
            if let Some(s) = slot
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
        count_schedules,
        CountSchedulesRequest,
        CountSchedulesResponse,
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
        count_workers,
        CountWorkersRequest,
        CountWorkersResponse,
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
    (
        describe_worker,
        DescribeWorkerRequest,
        DescribeWorkerResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        set_worker_deployment_manager,
        SetWorkerDeploymentManagerRequest,
        SetWorkerDeploymentManagerResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        pause_workflow_execution,
        PauseWorkflowExecutionRequest,
        PauseWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        unpause_workflow_execution,
        UnpauseWorkflowExecutionRequest,
        UnpauseWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        start_activity_execution,
        StartActivityExecutionRequest,
        StartActivityExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_activity_execution,
        DescribeActivityExecutionRequest,
        DescribeActivityExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_activity_execution,
        PollActivityExecutionRequest,
        PollActivityExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_activity_executions,
        ListActivityExecutionsRequest,
        ListActivityExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        count_activity_executions,
        CountActivityExecutionsRequest,
        CountActivityExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        request_cancel_activity_execution,
        RequestCancelActivityExecutionRequest,
        RequestCancelActivityExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        terminate_activity_execution,
        TerminateActivityExecutionRequest,
        TerminateActivityExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_activity_execution,
        DeleteActivityExecutionRequest,
        DeleteActivityExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        pause_activity_execution,
        PauseActivityExecutionRequest,
        PauseActivityExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        unpause_activity_execution,
        UnpauseActivityExecutionRequest,
        UnpauseActivityExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        reset_activity_execution,
        ResetActivityExecutionRequest,
        ResetActivityExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_activity_execution_options,
        UpdateActivityExecutionOptionsRequest,
        UpdateActivityExecutionOptionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        count_nexus_operation_executions,
        CountNexusOperationExecutionsRequest,
        CountNexusOperationExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        create_worker_deployment,
        CreateWorkerDeploymentRequest,
        CreateWorkerDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        create_worker_deployment_version,
        CreateWorkerDeploymentVersionRequest,
        CreateWorkerDeploymentVersionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_nexus_operation_execution,
        DeleteNexusOperationExecutionRequest,
        DeleteNexusOperationExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_nexus_operation_execution,
        DescribeNexusOperationExecutionRequest,
        DescribeNexusOperationExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_nexus_operation_executions,
        ListNexusOperationExecutionsRequest,
        ListNexusOperationExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_nexus_operation_execution,
        PollNexusOperationExecutionRequest,
        PollNexusOperationExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_workflow_execution_time_skipping,
        PollWorkflowExecutionTimeSkippingRequest,
        PollWorkflowExecutionTimeSkippingResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
            r.extensions_mut().insert(IsUserLongPoll);
        }
    );
    (
        request_cancel_nexus_operation_execution,
        RequestCancelNexusOperationExecutionRequest,
        RequestCancelNexusOperationExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        start_nexus_operation_execution,
        StartNexusOperationExecutionRequest,
        StartNexusOperationExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        terminate_nexus_operation_execution,
        TerminateNexusOperationExecutionRequest,
        TerminateNexusOperationExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_worker_deployment_version_compute_config,
        UpdateWorkerDeploymentVersionComputeConfigRequest,
        UpdateWorkerDeploymentVersionComputeConfigResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        validate_worker_deployment_version_compute_config,
        ValidateWorkerDeploymentVersionComputeConfigRequest,
        ValidateWorkerDeploymentVersionComputeConfigResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
}

proxier! {
    OperatorService; ALL_IMPLEMENTED_OPERATOR_SERVICE_RPCS; OperatorServiceClient; operator_client; defaults;
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
    CloudService; ALL_IMPLEMENTED_CLOUD_SERVICE_RPCS; CloudServiceClient; cloud_client; defaults;
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
    (set_service_account_namespace_access, cloudreq::SetServiceAccountNamespaceAccessRequest, cloudreq::SetServiceAccountNamespaceAccessResponse);
    (validate_account_audit_log_sink, cloudreq::ValidateAccountAuditLogSinkRequest, cloudreq::ValidateAccountAuditLogSinkResponse);
    (get_current_identity, cloudreq::GetCurrentIdentityRequest, cloudreq::GetCurrentIdentityResponse);
    (get_audit_logs, cloudreq::GetAuditLogsRequest, cloudreq::GetAuditLogsResponse);
    (create_account_audit_log_sink, cloudreq::CreateAccountAuditLogSinkRequest, cloudreq::CreateAccountAuditLogSinkResponse);
    (get_account_audit_log_sink, cloudreq::GetAccountAuditLogSinkRequest, cloudreq::GetAccountAuditLogSinkResponse);
    (get_account_audit_log_sinks, cloudreq::GetAccountAuditLogSinksRequest, cloudreq::GetAccountAuditLogSinksResponse);
    (update_account_audit_log_sink, cloudreq::UpdateAccountAuditLogSinkRequest, cloudreq::UpdateAccountAuditLogSinkResponse);
    (delete_account_audit_log_sink, cloudreq::DeleteAccountAuditLogSinkRequest, cloudreq::DeleteAccountAuditLogSinkResponse);
    (get_namespace_capacity_info, cloudreq::GetNamespaceCapacityInfoRequest, cloudreq::GetNamespaceCapacityInfoResponse);
    (create_billing_report, cloudreq::CreateBillingReportRequest, cloudreq::CreateBillingReportResponse);
    (get_billing_report, cloudreq::GetBillingReportRequest, cloudreq::GetBillingReportResponse);
    (get_custom_roles, cloudreq::GetCustomRolesRequest, cloudreq::GetCustomRolesResponse);
    (get_custom_role, cloudreq::GetCustomRoleRequest, cloudreq::GetCustomRoleResponse);
    (create_custom_role, cloudreq::CreateCustomRoleRequest, cloudreq::CreateCustomRoleResponse);
    (update_custom_role, cloudreq::UpdateCustomRoleRequest, cloudreq::UpdateCustomRoleResponse);
    (delete_custom_role, cloudreq::DeleteCustomRoleRequest, cloudreq::DeleteCustomRoleResponse);
    (get_user_namespace_assignments, cloudreq::GetUserNamespaceAssignmentsRequest, cloudreq::GetUserNamespaceAssignmentsResponse);
    (get_service_account_namespace_assignments, cloudreq::GetServiceAccountNamespaceAssignmentsRequest, cloudreq::GetServiceAccountNamespaceAssignmentsResponse);
    (get_user_group_namespace_assignments, cloudreq::GetUserGroupNamespaceAssignmentsRequest, cloudreq::GetUserGroupNamespaceAssignmentsResponse);
}

proxier! {
    TestService; ALL_IMPLEMENTED_TEST_SERVICE_RPCS; TestServiceClient; test_client; defaults;
    (lock_time_skipping, LockTimeSkippingRequest, LockTimeSkippingResponse);
    (unlock_time_skipping, UnlockTimeSkippingRequest, UnlockTimeSkippingResponse);
    (sleep, SleepRequest, SleepResponse);
    (sleep_until, SleepUntilRequest, SleepResponse);
    (unlock_time_skipping_with_sleep, SleepRequest, SleepResponse);
    (get_current_time, (), GetCurrentTimeResponse);
}

proxier! {
    HealthService; ALL_IMPLEMENTED_HEALTH_SERVICE_RPCS; HealthClient; health_client;
    (check, HealthCheckRequest, HealthCheckResponse);
    (watch, HealthCheckRequest, tonic::codec::Streaming<HealthCheckResponse>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientOptions, ConnectionOptions};
    use std::collections::HashSet;
    use temporalio_common::{
        protos::temporal::api::{
            operatorservice::v1::DeleteNamespaceRequest, workflowservice::v1::ListNamespacesRequest,
        },
        worker::WorkerTaskTypes,
    };
    use tonic::IntoRequest;
    use url::Url;
    use uuid::Uuid;

    #[test]
    fn payload_limits_warn_only_vs_error() {
        use temporalio_common::protos::temporal::api::{
            common::v1::{Payload, Payloads},
            workflowservice::v1::StartWorkflowExecutionRequest,
        };
        let big = Payloads {
            payloads: vec![Payload {
                data: vec![0u8; 1000],
                ..Default::default()
            }],
        };
        let new_req = || {
            StartWorkflowExecutionRequest {
                input: Some(big.clone()),
                ..Default::default()
            }
            .into_request()
        };

        // warn thresholds = 1 byte. No per-call error limits: over-warn is allowed (warn-only).
        assert!(validate_request_payload_limits(&new_req(), 1, 1).is_ok());

        // With per-call error limits below the payload size: rejected, carrying the typed violation.
        let mut req = new_req();
        req.extensions_mut()
            .insert(PayloadErrorLimits { blob: 10, memo: 10 });
        let err = validate_request_payload_limits(&req, 1, 1).unwrap_err();
        let violation =
            crate::payload_limit_violation_from(&err).expect("violation carried on status");
        assert_eq!(violation.path, "input");
        assert_eq!(
            violation.class,
            temporalio_common::payload_limits::LimitClass::Blob
        );
        assert!(violation.size > violation.limit);

        // A zero error threshold means "no limit" for that class, so it does not reject.
        let mut req = new_req();
        req.extensions_mut()
            .insert(PayloadErrorLimits { blob: 0, memo: 0 });
        assert!(validate_request_payload_limits(&req, 1, 1).is_ok());

        // Zero warn thresholds disable warnings (and there are no error limits): always ok.
        assert!(validate_request_payload_limits(&new_req(), 0, 0).is_ok());
    }

    // Just to help make sure some stuff compiles. Not run.
    #[allow(dead_code)]
    async fn raw_client_retry_compiles() {
        let opts = ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap())
            .client_name("test")
            .client_version("0.0.0")
            .build();
        let connection = Connection::connect(opts).await.unwrap();
        let mut client = Client::new(connection, ClientOptions::new("default").build()).unwrap();

        let list_ns_req = ListNamespacesRequest::default();
        let wf_client = client.workflow_client();
        let fact = move |req| {
            let mut c = wf_client.clone();
            async move { c.list_namespaces(req).await }.boxed()
        };
        client
            .call("whatever", fact, Request::new(list_ns_req.clone()))
            .await
            .unwrap();

        // Operator svc method
        let op_del_ns_req = DeleteNamespaceRequest::default();
        let op_client = client.operator_client();
        let fact = move |req| {
            let mut c = op_client.clone();
            async move { c.delete_namespace(req).await }.boxed()
        };
        client
            .call("whatever", fact, Request::new(op_del_ns_req.clone()))
            .await
            .unwrap();

        // Cloud svc method
        let cloud_del_ns_req = cloudreq::DeleteNamespaceRequest::default();
        let cloud_client = client.cloud_client();
        let fact = move |req| {
            let mut c = cloud_client.clone();
            async move { c.delete_namespace(req).await }.boxed()
        };
        client
            .call("whatever", fact, Request::new(cloud_del_ns_req.clone()))
            .await
            .unwrap();

        // Verify calling through traits works
        client
            .list_namespaces(list_ns_req.into_request())
            .await
            .unwrap();
        // Have to disambiguate operator and cloud service
        OperatorService::delete_namespace(&mut client, op_del_ns_req.into_request())
            .await
            .unwrap();
        CloudService::delete_namespace(&mut client, cloud_del_ns_req.into_request())
            .await
            .unwrap();
        client.get_current_time(().into_request()).await.unwrap();
        client
            .check(HealthCheckRequest::default().into_request())
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
            "../../protos/protos/api_upstream/temporal/api/workflowservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_WORKFLOW_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_operator_service_methods_implemented() {
        let proto_def = include_str!(
            "../../protos/protos/api_upstream/temporal/api/operatorservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_OPERATOR_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_cloud_service_methods_implemented() {
        let proto_def = include_str!(
            "../../protos/protos/api_cloud_upstream/temporal/api/cloud/cloudservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_CLOUD_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_test_service_methods_implemented() {
        let proto_def = include_str!(
            "../../protos/protos/testsrv_upstream/temporal/api/testservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_TEST_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_health_service_methods_implemented() {
        let proto_def = include_str!("../../protos/protos/grpc/health/v1/health.proto");
        verify_methods(proto_def, ALL_IMPLEMENTED_HEALTH_SERVICE_RPCS);
    }

    #[tokio::test]
    async fn can_mock_services() {
        #[derive(Clone)]
        struct MyFakeServices {}
        impl RawGrpcCaller for MyFakeServices {}
        impl WorkflowService for MyFakeServices {
            fn list_namespaces(
                &mut self,
                _request: Request<ListNamespacesRequest>,
            ) -> BoxFuture<'_, Result<Response<ListNamespacesResponse>, Status>> {
                async {
                    Ok(Response::new(ListNamespacesResponse {
                        namespaces: vec![DescribeNamespaceResponse {
                            failover_version: 12345,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }))
                }
                .boxed()
            }
        }
        impl OperatorService for MyFakeServices {}
        impl CloudService for MyFakeServices {}
        impl TestService for MyFakeServices {}
        // Health service isn't possible to create a default impl for.
        impl HealthService for MyFakeServices {
            fn check(
                &mut self,
                _request: tonic::Request<HealthCheckRequest>,
            ) -> BoxFuture<'_, Result<tonic::Response<HealthCheckResponse>, tonic::Status>>
            {
                todo!()
            }
            fn watch(
                &mut self,
                _request: tonic::Request<HealthCheckRequest>,
            ) -> BoxFuture<
                '_,
                Result<
                    tonic::Response<tonic::codec::Streaming<HealthCheckResponse>>,
                    tonic::Status,
                >,
            > {
                todo!()
            }
        }
        let mut mocked_client = TemporalServiceClient::from_services(
            Box::new(MyFakeServices {}),
            Box::new(MyFakeServices {}),
            Box::new(MyFakeServices {}),
            Box::new(MyFakeServices {}),
            Box::new(MyFakeServices {}),
        );
        let r = mocked_client
            .list_namespaces(ListNamespacesRequest::default().into_request())
            .await
            .unwrap();
        assert_eq!(r.into_inner().namespaces[0].failover_version, 12345);
    }

    #[rstest::rstest]
    #[case::with_versioning(true)]
    #[case::without_versioning(false)]
    #[tokio::test]
    async fn eager_reservations_attach_deployment_options(#[case] use_worker_versioning: bool) {
        use crate::worker::{MockClientWorker, MockSlot};
        use temporalio_common::{
            protos::temporal::api::enums::v1::WorkerVersioningMode,
            worker::{WorkerDeploymentOptions, WorkerDeploymentVersion},
        };

        let expected_mode = if use_worker_versioning {
            WorkerVersioningMode::Versioned
        } else {
            WorkerVersioningMode::Unversioned
        };

        #[derive(Clone)]
        struct MyFakeServices {
            client_worker_set: Arc<ClientWorkerSet>,
            expected_mode: WorkerVersioningMode,
        }
        impl RawGrpcCaller for MyFakeServices {}
        impl RawClientProducer for MyFakeServices {
            fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
                Some(self.client_worker_set.clone())
            }
            fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
                Box::new(MyFakeWfClient {
                    expected_mode: self.expected_mode,
                })
            }
            fn operator_client(&mut self) -> Box<dyn OperatorService> {
                unimplemented!()
            }
            fn cloud_client(&mut self) -> Box<dyn CloudService> {
                unimplemented!()
            }
            fn test_client(&mut self) -> Box<dyn TestService> {
                unimplemented!()
            }
            fn health_client(&mut self) -> Box<dyn HealthService> {
                unimplemented!()
            }
        }

        let deployment_opts = WorkerDeploymentOptions::new(WorkerDeploymentVersion {
            deployment_name: "test-deployment".to_string(),
            build_id: "test-build-123".to_string(),
        })
        .use_worker_versioning(use_worker_versioning)
        .build();

        let mut mock_provider = MockClientWorker::new();
        mock_provider
            .expect_namespace()
            .return_const("test-namespace".to_string());
        mock_provider
            .expect_task_queue()
            .return_const("test-task-queue".to_string());
        let mut mock_slot = MockSlot::new();
        mock_slot.expect_schedule_wft().returning(|_| Ok(()));
        mock_provider
            .expect_try_reserve_wft_slot()
            .return_once(|| Some(Box::new(mock_slot)));
        mock_provider
            .expect_deployment_options()
            .return_const(Some(deployment_opts.clone()));
        mock_provider.expect_heartbeat_enabled().return_const(false);
        let uuid = Uuid::new_v4();
        mock_provider
            .expect_worker_instance_key()
            .return_const(uuid);
        mock_provider
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: true,
            });

        let client_worker_set = Arc::new(ClientWorkerSet::new());
        client_worker_set
            .register_worker(Arc::new(mock_provider), true)
            .unwrap();

        #[derive(Clone)]
        struct MyFakeWfClient {
            expected_mode: WorkerVersioningMode,
        }
        impl WorkflowService for MyFakeWfClient {
            fn start_workflow_execution(
                &mut self,
                request: tonic::Request<StartWorkflowExecutionRequest>,
            ) -> BoxFuture<'_, Result<tonic::Response<StartWorkflowExecutionResponse>, tonic::Status>>
            {
                let req = request.into_inner();
                let expected_mode = self.expected_mode;

                assert!(
                    req.eager_worker_deployment_options.is_some(),
                    "eager_worker_deployment_options should be populated"
                );

                let opts = req.eager_worker_deployment_options.as_ref().unwrap();
                assert_eq!(opts.deployment_name, "test-deployment");
                assert_eq!(opts.build_id, "test-build-123");
                assert_eq!(opts.worker_versioning_mode, expected_mode as i32);

                async { Ok(Response::new(StartWorkflowExecutionResponse::default())) }.boxed()
            }
        }

        let mut mfs = MyFakeServices {
            client_worker_set,
            expected_mode,
        };

        // Create a request with eager execution enabled
        let req = StartWorkflowExecutionRequest {
            namespace: "test-namespace".to_string(),
            workflow_id: "test-wf-id".to_string(),
            workflow_type: Some(
                temporalio_common::protos::temporal::api::common::v1::WorkflowType {
                    name: "test-workflow".to_string(),
                },
            ),
            task_queue: Some(TaskQueue {
                name: "test-task-queue".to_string(),
                kind: 0,
                normal_name: String::new(),
            }),
            request_eager_execution: true,
            ..Default::default()
        };

        mfs.start_workflow_execution(req.into_request())
            .await
            .unwrap();
    }

    /// Tests that Connection's RawClientProducer impl correctly provides worker info
    /// so that eager workflow start can reserve a slot and dispatch the WFT.
    #[tokio::test]
    async fn connection_eager_start_dispatches_wft() {
        use crate::{
            ConnectionOptions,
            callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse},
            worker::{MockClientWorker, MockSlot},
        };
        use prost::Message;
        use std::sync::atomic::{AtomicBool, Ordering};
        use temporalio_common::protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;

        let dispatched = Arc::new(AtomicBool::new(false));
        let dispatched_clone = dispatched.clone();

        // Create a callback-based service that returns an eager_workflow_task in the response
        let service_override = CallbackBasedGrpcService {
            callback: Arc::new(|_req| {
                Box::pin(async {
                    let resp = StartWorkflowExecutionResponse {
                        run_id: "test-run-id".to_string(),
                        eager_workflow_task: Some(PollWorkflowTaskQueueResponse {
                            task_token: vec![1, 2, 3],
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    let proto = resp.encode_to_vec();
                    Ok(GrpcSuccessResponse {
                        headers: Default::default(),
                        proto,
                    })
                })
            }),
        };

        let opts = ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
            .skip_get_system_info(true)
            .service_override(service_override)
            .dns_load_balancing(None)
            .build();
        let mut connection = crate::Connection::connect(opts).await.unwrap();

        // Register a mock worker on the connection's worker set
        let mut mock_worker = MockClientWorker::new();
        mock_worker
            .expect_namespace()
            .return_const("default".to_string());
        mock_worker
            .expect_task_queue()
            .return_const("test-tq".to_string());
        mock_worker
            .expect_deployment_options()
            .return_const(None::<temporalio_common::worker::WorkerDeploymentOptions>);
        mock_worker.expect_heartbeat_enabled().return_const(false);
        let uuid = Uuid::new_v4();
        mock_worker.expect_worker_instance_key().return_const(uuid);
        mock_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: false,
                enable_remote_activities: false,
                enable_nexus: false,
            });

        let mut mock_slot = MockSlot::new();
        mock_slot.expect_schedule_wft().returning(move |_| {
            dispatched_clone.store(true, Ordering::SeqCst);
            Ok(())
        });
        mock_worker
            .expect_try_reserve_wft_slot()
            .return_once(|| Some(Box::new(mock_slot)));

        connection
            .workers()
            .register_worker(Arc::new(mock_worker), true)
            .unwrap();

        // Make an eager start_workflow_execution call through Connection
        let req = StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "test-wf".to_string(),
            workflow_type: Some(
                temporalio_common::protos::temporal::api::common::v1::WorkflowType {
                    name: "test-workflow".to_string(),
                },
            ),
            task_queue: Some(TaskQueue {
                name: "test-tq".to_string(),
                kind: 0,
                normal_name: String::new(),
            }),
            request_eager_execution: true,
            ..Default::default()
        };

        connection
            .start_workflow_execution(req.into_request())
            .await
            .unwrap();

        assert!(
            dispatched.load(Ordering::SeqCst),
            "Eager workflow task should have been dispatched to the worker"
        );
    }
}

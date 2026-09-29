#include "base.h"

namespace NKikimr::NGRpcService {

// The type-independent part of the grpc request wrappers is instantiated once
// per (context interface, runtime event) combination here, instead of once per
// grpc method in every translation unit.
template class TGRpcRequestWrapperBase<IRequestOpCtx, TEvProxyRuntimeEventWithType<NRuntimeEvents::EType::COMMON>>;
template class TGRpcRequestWrapperBase<IRequestOpCtx, TEvProxyRuntimeEventWithType<NRuntimeEvents::EType::BOOTSTRAP_CLUSTER>>;
template class TGRpcRequestWrapperBase<IRequestNoOpCtx, TEvProxyRuntimeEventWithType<NRuntimeEvents::EType::COMMON>>;
template class TGRpcRequestWrapperBase<IRequestNoOpCtx, TEvProxyRuntimeEventWithType<NRuntimeEvents::EType::BOOTSTRAP_CLUSTER>>;
template class TGrpcResponseSenderImpl<TEvProxyRuntimeEventWithType<NRuntimeEvents::EType::COMMON>>;
template class TGrpcResponseSenderImpl<TEvProxyRuntimeEventWithType<NRuntimeEvents::EType::BOOTSTRAP_CLUSTER>>;

} // namespace NKikimr::NGRpcService

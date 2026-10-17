#include "grpc_request_proxy_handle_methods.h"

namespace NKikimr::NGRpcService {

void TGRpcRequestProxyHandleMethods::Handle(TEvStreamPQWriteRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NGRpcProxy::V1::GetPQWriteServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvStreamPQMigrationReadRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NGRpcProxy::V1::GetPQReadServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvStreamTopicWriteRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NGRpcProxy::V1::GetPQWriteServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvStreamTopicReadRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NGRpcProxy::V1::GetPQReadServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvStreamTopicDirectReadRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NGRpcProxy::V1::GetPQReadServiceActorID(), ev->Release().Release());
}

} // namespace NKikimr::NGRpcService

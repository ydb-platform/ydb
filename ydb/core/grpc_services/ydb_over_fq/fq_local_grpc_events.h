#pragma once

#include "local_grpc_context.h"

#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/core/grpc_services/service_fq.h>

namespace NKikimr::NGRpcService {

namespace NYdbOverFq {

enum EEventTypes {
    EvCreateQueryRequest = NFq::YqEventSubspaceBegin(NFq::TYqEventSubspace::TableOverFq),
    EvCreateQueryResponse,
    EvGetQueryStatusRequest,
    EvGetQueryStatusResponse,
    EvDescribeQueryRequest,
    EvDescribeQueryResponse,
    EvGetResultDataRequest,
    EvGetResultDataResponse,
    EvListBindingsRequest,
    EvListBindingsResponse,
    EvDescribeBindingRequest,
    EvDescribeBindingResponse,
};

}

#define DEFINE_LOCAL_GRPC_CALL_IMPL(Req, Resp, Ctor) \
template <> \
struct TLocalGrpcCall<Req> \
    : public TLocalGrpcCallBase<Req, Resp> { \
    \
    using TBase = TLocalGrpcCallBase<Req, Resp>; \
    \
    static std::unique_ptr<TEvProxyRuntimeEvent> MakeRequest( \
        TRequest&& request, \
        std::shared_ptr<IRequestCtx>&& baseRequest, \
        std::function<void(const TResponse&)>&& replyCallback) { \
        return Ctor(TBase::MakeContext(std::move(request), std::move(baseRequest), std::move(replyCallback))); \
    } \
};

#define DEFINE_EVENT(Event) \
template <> \
class TEvent<FederatedQuery::Event> : public TEventBase<FederatedQuery::Event, NYdbOverFq::Ev##Event> { \
    using TBase = TEventBase<FederatedQuery::Event, NYdbOverFq::Ev##Event>; \
    using TProto = FederatedQuery::Event; \
    \
    using TBase::TBase; \
}; \
using TEv##Event = TEvent<FederatedQuery::Event>;

#define DEFINE_LOCAL_GRPC_CALL(Method) \
    DEFINE_LOCAL_GRPC_CALL_IMPL(FederatedQuery::Method##Request, FederatedQuery::Method##Response, CreateFederatedQuery##Method##RequestOperationCall) \
    using TLocalGrpc##Method##Call = TLocalGrpcCall<FederatedQuery::Method##Request>; \
    DEFINE_EVENT(Method##Request) \
    DEFINE_EVENT(Method##Response)

DEFINE_LOCAL_GRPC_CALL(CreateQuery)
DEFINE_LOCAL_GRPC_CALL(GetQueryStatus)
DEFINE_LOCAL_GRPC_CALL(DescribeQuery)
DEFINE_LOCAL_GRPC_CALL(GetResultData)
DEFINE_LOCAL_GRPC_CALL(ListBindings)
DEFINE_LOCAL_GRPC_CALL(DescribeBinding)

#undef DEFINE_EVENT
#undef DEFINE_LOCAL_GRPC_CALL
#undef DEFINE_LOCAL_GRPC_CALL_IMPL

}

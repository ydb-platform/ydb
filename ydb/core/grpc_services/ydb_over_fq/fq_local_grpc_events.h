#pragma once

#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/core/grpc_services/local_grpc/local_grpc.h>
#include <ydb/core/grpc_services/service_fq.h>

namespace NKikimr::NGRpcService {

namespace NYdbOverFq {

template <typename TReq, typename TResp>
class TLocalGrpcFqContext : public NLocalGrpc::TContext<TReq, TResp> {
public:
    using TBase = NLocalGrpc::TContext<TReq, TResp>;

    TLocalGrpcFqContext(
        TReq&& request, std::shared_ptr<IRequestCtx> baseRequest,
        std::function<void(const TResp&)> replyCallback)
        : TBase{std::move(request), std::move(baseRequest), std::move(replyCallback)}
        , Scope_{"yandexcloud:/" + TBase::GetBaseRequest().GetDatabaseName().GetOrElse("/")}
    {}

    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override {
        if (key == "x-ydb-fq-project") {
            return {Scope_};
        }

        return TBase::GetPeerMetaValues(key);
    }

    TString GetPeer() const override {
        return TBase::GetBaseRequest().GetPeerName();
    }

private:
    TString Scope_;
};

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
} // namespace NYdbOverFq

#define DEFINE_LOCAL_GRPC_CALL_IMPL(Req, Resp, Res, Ctor) \
template <> \
struct NLocalGrpc::TCall<Req> \
    : public TCallBase<Req, Resp, Res> { \
    \
    using TBase = TCallBase<Req, Resp, Res>; \
    \
    static std::unique_ptr<TEvProxyRuntimeEvent> MakeRequest( \
        TRequest&& request, \
        std::shared_ptr<IRequestCtx>&& baseRequest, \
        std::function<void(const TResponse&)>&& replyCallback) { \
        return Ctor(new NYdbOverFq::TLocalGrpcFqContext(std::move(request), std::move(baseRequest), std::move(replyCallback))); \
    } \
};

#define DEFINE_EVENT(Event) \
template <> \
class NLocalGrpc::TEvent<FederatedQuery::Event> : public TEventBase<FederatedQuery::Event, NYdbOverFq::Ev##Event> { \
    using TBase = TEventBase<FederatedQuery::Event, NYdbOverFq::Ev##Event>; \
    \
    using TBase::TBase; \
}; \
using TEvFq##Event = NLocalGrpc::TEvent<FederatedQuery::Event>;

#define DEFINE_LOCAL_GRPC_CALL(Method) \
    DEFINE_LOCAL_GRPC_CALL_IMPL( \
        FederatedQuery::Method##Request, FederatedQuery::Method##Response, FederatedQuery::Method##Result, \
        CreateFederatedQuery##Method##RequestOperationCall) \
    using TFq##Method##Call = NLocalGrpc::TCall<FederatedQuery::Method##Request>; \
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

} // namespace NKikimr::NGRpcService

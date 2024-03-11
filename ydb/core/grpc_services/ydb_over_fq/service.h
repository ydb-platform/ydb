#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/core/fq/libs/events/event_subspace.h>
#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

namespace NYdbOverFq {

template <typename TMsg, ui32 EMsgType>
class TEventBase : public TEventLocal<TEventBase<TMsg, EMsgType>, EMsgType> {
public:
    TEventBase() = default;

    TEventBase(TMsg message)
        : Message{std::move(message)}
    {}

    TMsg Message;
};

template <typename TMsg>
class TEvent;

enum EEventTypes {
    EvCreateQueryRequest = NFq::YqEventSubspaceBegin(NFq::TYqEventSubspace::TableOverFq),
    EvCreateQueryResponse,
    EvGetQueryStatusRequest,
    EvGetQueryStatusResponse,
    EvDescribeQueryRequest,
    EvDescribeQueryResponse,
    EvGetResultDataRequest,
    EvGetResultDataResponse,
};

#define DEFINE_EVENT(Event) \
template <> \
class TEvent<FederatedQuery::Event> : public TEventBase<FederatedQuery::Event, Ev##Event> { \
    using TBase = TEventBase<FederatedQuery::Event, Ev##Event>; \
    using TBase::TBase; \
}; \
using TEv##Event = TEvent<FederatedQuery::Event>;

#define DEFINE_REQ_RESP(Name) \
DEFINE_EVENT(Name##Request)  \
DEFINE_EVENT(Name##Response) \

DEFINE_REQ_RESP(CreateQuery)
DEFINE_REQ_RESP(GetQueryStatus)
DEFINE_REQ_RESP(DescribeQuery)
DEFINE_REQ_RESP(GetResultData)

#undef DEFINE_REQ_RESP
#undef DEFINE_EVENT

// table
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetCreateSessionExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetKeepAliveExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetDescribeTableExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetExplainDataQueryExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetExecuteDataQueryExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetListDirectoryExecutor(NActors::TActorId grpcProxyId);

} // namespace NYdbOverFq

} // namespace NKikimr::NGRpcService

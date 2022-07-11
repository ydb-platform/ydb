#pragma once

#include <ydb/core/sys_view/common/events.h>

#include <library/cpp/grpc/server/grpc_counters.h>

namespace NKikimr {
namespace NGRpcService {

class TServiceCounterCB {
public:
    TServiceCounterCB(::NMonitoring::TDynamicCounterPtr counters, TActorSystem *actorSystem);

    NGrpc::ICounterBlockPtr operator()(const char* serviceName, const char* requestName,
        bool percentile = true, bool streaming = false) const;

private:
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TActorSystem* ActorSystem;
    TActorId ActorId;
};

inline TServiceCounterCB CreateCounterCb(::NMonitoring::TDynamicCounterPtr counters,
    TActorSystem *actorSystem)
{
    return TServiceCounterCB(std::move(counters), actorSystem);
}

TIntrusivePtr<NSysView::IDbCounters> CreateGRpcDbCounters(
    ::NMonitoring::TDynamicCounterPtr externalGroup,
    ::NMonitoring::TDynamicCounterPtr internalGroup);

} // namespace NGRpcService
} // namespace NKikimr

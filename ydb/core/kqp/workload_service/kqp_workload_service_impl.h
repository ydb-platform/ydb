#pragma once

#include "kqp_workload_service_tables_impl.h"

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NKqp::NWorkload {

constexpr TDuration LEASE_DURATION = TDuration::Seconds(30);

namespace NQueue {

class IState : public TThrRefBase {
public:
    virtual bool TablesRequired() const = 0;
    virtual ui64 GetLocalPoolSize() const = 0;

    virtual void OnPreparingFinished(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) = 0;

    virtual bool PlaceRequest(const NActors::TActorId& workerActorId, const TString& sessionId) = 0;
    virtual void CleanupRequest(const NActors::TActorId& workerActorId, const TString& sessionId) = 0;
    virtual void RefreshState(bool refreshRequired = false) = 0;

    virtual void Handle(TEvPrivate::TEvCancelRequest::TPtr) {};
    virtual void Handle(TEvPrivate::TEvRefreshPoolStateResponse::TPtr) {};
    virtual void Handle(TEvPrivate::TEvDelayRequestResponse::TPtr) {};
    virtual void Handle(TEvPrivate::TEvStartRequestResponse::TPtr) {};
    virtual void Handle(TEvPrivate::TEvCleanupRequestsResponse::TPtr) {};
};

using TStatePtr = TIntrusivePtr<IState>;

TStatePtr CreateState(const NActors::TActorContext& actorContext, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters);

}  // NQueue

}  // NKikimr::NKqp::NWorkload

#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/dq_sync_compute_actor_base.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>

namespace NKikimr::NKqp::NScheduler {

struct TSchedulerEntity;

class TComputeScheduler {
public:
    explicit TComputeScheduler(TIntrusivePtr<TKqpCounters> counters = {});
    ~TComputeScheduler();

    void SetCapacity(ui64 cores);

    void UpdatePoolShare(TString poolName, double share, TMonotonic now, std::optional<double> resourceWeight);
    void UpdatePerQueryShare(TString name, double share, TMonotonic now);

    void SetMaxDeviation(TDuration); // for testing
    void SetForgetInterval(TDuration);

    THolder<TSchedulerEntity> Enroll(TString poolName, i64 weight, TMonotonic now);

    void AdvanceTime(TMonotonic now);

    void Unregister(THolder<TSchedulerEntity>& entity, TMonotonic now);

    bool Disabled(TString poolName);
    void Disable(TString poolName, TMonotonic now);

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

struct TKqpComputeSchedulerEvents {
    enum EKqpComputeSchedulerEvents {
        EvUnregister = EventSpaceBegin(TKikimrEvents::ES_KQP) + 400,
        EvNewPool,
        EvPingPool,
    };
};

struct TEvSchedulerNewPool : public TEventLocal<TEvSchedulerNewPool, TKqpComputeSchedulerEvents::EvNewPool> {
    TString DatabaseId;
    TString Pool;

    TEvSchedulerNewPool(TString databaseId, TString pool)
        : DatabaseId(databaseId)
        , Pool(pool)
    {
    }
};

struct TSchedulerActorOptions {
    std::shared_ptr<TComputeScheduler> Scheduler;
    TDuration AdvanceTimeInterval;
    TDuration ForgetOverflowTimeout;
    TDuration ActivePoolPollingTimeout;
    TIntrusivePtr<NKikimr::NKqp::TKqpCounters> Counters;
};

IActor* CreateSchedulerActor(TSchedulerActorOptions);

} // namespace NKikimr::NKqp::NScheduler

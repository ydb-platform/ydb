#pragma once

#include "fwd.h"

#include <ydb/core/base/events.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>

namespace NKikimr::NKqp::NScheduler {

class TComputeScheduler : public std::enable_shared_from_this<TComputeScheduler> {
public:
    TComputeScheduler(TIntrusivePtr<TKqpCounters> counters, const TDelayParams& delayParams);

    void SetTotalCpuLimit(ui64 cpu);
    ui64 GetTotalCpuLimit() const;

    void AddOrUpdateDatabase(const TString& databaseId, const NHdrf::TStaticAttributes& attrs);

    void AddOrUpdatePool(const TString& databaseId, const TString& poolId, const NHdrf::TStaticAttributes& attrs);

    NHdrf::NDynamic::TQueryPtr AddOrUpdateQuery(const TString& databaseId, const TString& poolId, const NHdrf::TQueryId& queryId, const NHdrf::TStaticAttributes& attrs);
    void RemoveQuery(const NHdrf::NDynamic::TQueryPtr& query);

    void UpdateFairShare();

private:
    TRWMutex Mutex;
    NHdrf::NDynamic::TRootPtr Root;                                // protected by Mutex
    THashMap<NHdrf::TQueryId, NHdrf::NDynamic::TQueryPtr> Queries; // protected by Mutex

    const TDelayParams DelayParams;
    TIntrusivePtr<TKqpCounters> KqpCounters;

    struct {
        NMonitoring::TDynamicCounters::TCounterPtr UpdateFairShare;
    } Counters;
};

using TComputeSchedulerPtr = std::shared_ptr<TComputeScheduler>;

struct TOptions {
    TIntrusivePtr<TKqpCounters> Counters;
    TDelayParams DelayParams;
    TDuration UpdateFairSharePeriod;
};

struct TEvents {
    enum : ui32 {
        EvAddDatabase = EventSpaceBegin(TKikimrEvents::ES_KQP) + 400,
        EvRemoveDatabase,
        EvAddPool,
        EvRemovePool,
        EvAddQuery,
        EvRemoveQuery,
        EvQueryResponse,
    };
};

struct TEvAddDatabase : public TEventLocal<TEvAddDatabase, TEvents::EvAddDatabase> {
    TString Id;
    double Weight = 1.;
};

struct TEvRemoveDatabase : public TEventLocal<TEvRemoveDatabase, TEvents::EvRemoveDatabase> {
    TString Id;
};

struct TEvAddPool : public TEventLocal<TEvAddPool, TEvents::EvAddPool> {
    TEvAddPool(const TString& databaseId, const TString& poolId) : DatabaseId(databaseId), PoolId(poolId) {}
    TEvAddPool(const TString& databaseId, const TString& poolId, const NResourcePool::TPoolSettings& params) : DatabaseId(databaseId), PoolId(poolId), Params(params) {}

    TString DatabaseId;
    TString PoolId;
    NResourcePool::TPoolSettings Params;
    double Weight = 1.;
};

struct TEvRemovePool : public TEventLocal<TEvRemovePool, TEvents::EvRemovePool> {
    TString DatabaseId;
    TString PoolId;
};

struct TEvAddQuery : public TEventLocal<TEvAddQuery, TEvents::EvAddQuery> {
    TString DatabaseId;
    TString PoolId;
    NHdrf::TQueryId QueryId;
    double Weight = 1.;
};

struct TEvRemoveQuery : public TEventLocal<TEvRemoveQuery, TEvents::EvRemoveQuery> {
    NHdrf::NDynamic::TQueryPtr Query;
};

struct TEvQueryResponse : public TEventLocal<TEvQueryResponse, TEvents::EvQueryResponse> {
    NHdrf::NDynamic::TQueryPtr Query;
};

} // namespace NKikimr::NKqp::NScheduler

namespace NKikimr::NKqp {
    IActor* CreateKqpComputeSchedulerService(const NScheduler::TOptions& options);
}

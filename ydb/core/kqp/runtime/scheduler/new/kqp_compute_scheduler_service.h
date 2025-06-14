#pragma once

#include "fwd.h"

#include <ydb/core/base/events.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>

namespace NKikimr::NKqp::NScheduler {

class TComputeScheduler : public std::enable_shared_from_this<TComputeScheduler> {
public:
    explicit TComputeScheduler(TIntrusivePtr<TKqpCounters> counters);

    TSchedulableTaskFactory CreateSchedulableTaskFactory();

    void SetTotalCpuLimit(ui64 cpu);
    ui64 GetTotalCpuLimit() const;

    void AddOrUpdateDatabase(const TString& databaseId, const NHdrf::TStaticAttributes& attrs);

    void AddOrUpdatePool(const TString& databaseId, const TString& poolId, const NHdrf::TStaticAttributes& attrs);

    void AddOrUpdateQuery(const TString& databaseId, const TString& poolId, const NHdrf::TQueryId& queryId, const NHdrf::TStaticAttributes& attrs);
    void RemoveQuery(const TString& databaseId, const TString& poolId, const NHdrf::TQueryId& queryId);

    void UpdateFairShare();

private:
    NHdrf::NDynamic::TQueryPtr GetQuery(const NHdrf::TQueryId& queryId);

private:
    TRWMutex Mutex;
    NHdrf::NDynamic::TRootPtr Root;                                        // protected by Mutex
    THashMap<NHdrf::TQueryId, NHdrf::NDynamic::TQueryPtr> Queries;         // protected by Mutex
    THashMap<NHdrf::TQueryId, NHdrf::NDynamic::TQueryPtr> DetachedQueries; // protected by Mutex
    NHdrf::NDynamic::TPoolPtr DetachedPool;                                // protected by Mutex

    TIntrusivePtr<TKqpCounters> KqpCounters;

    struct {
        NMonitoring::TDynamicCounters::TCounterPtr UpdateFairShare;
    } Counters;
};

using TComputeSchedulerPtr = std::shared_ptr<TComputeScheduler>;

struct TOptions {
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
    TString DatabaseId;
    TString PoolId;
    NHdrf::TQueryId QueryId;
};

} // namespace NKikimr::NKqp::NScheduler

namespace NKikimr::NKqp {
    IActor* CreateKqpComputeSchedulerService(const NScheduler::TComputeSchedulerPtr& scheduler, const NScheduler::TOptions& options);
}

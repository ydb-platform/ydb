#pragma once

#include "fwd.h"

#include <ydb/core/base/events.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>

namespace NKikimr::NKqp::NScheduler {

class TComputeScheduler : public std::enable_shared_from_this<TComputeScheduler> {
public:
    TComputeScheduler(const TIntrusivePtr<TKqpCounters>& counters, const TDelayParams& delayParams,
        NHdrf::NSnapshot::ELeafFairShare fairShareMode = NHdrf::NSnapshot::ELeafFairShare::EQUAL_TO_PARENT);

    void SetTotalCpuLimit(ui64 cpu);
    ui64 GetTotalCpuLimit() const;

    void AddOrUpdateDatabase(const NHdrf::TDatabaseId& databaseId, const NHdrf::TStaticAttributes& attrs);

    void AddOrUpdatePool(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId, const NHdrf::TStaticAttributes& attrs);

    NHdrf::NDynamic::TQueryPtr AddOrUpdateQuery(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId, const NHdrf::TQueryId& queryId, const NHdrf::TStaticAttributes& attrs);
    NHdrf::NDynamic::TQueryPtr GetReadQuery(const NHdrf::TPoolId& poolId) const;
    bool RemoveQuery(const NHdrf::TQueryId& queryId);

    void UpdateFairShare();

private:
    static constexpr NHdrf::TQueryId READ_QUERY_ID = -1;

    TRWMutex Mutex;
    NHdrf::NDynamic::TRootPtr Root;                                // protected by Mutex
    THashMap<NHdrf::TQueryId, NHdrf::NDynamic::TQueryPtr> Queries; // protected by Mutex

    // Special virtual queries per each pool to create SchedulableRead upon them, used for datashards and columnshards.
    // TODO: get rid of read queries - just pass somehow the real query to datashards.
    THashMap<NHdrf::TPoolId, NHdrf::NDynamic::TQueryPtr> ReadQueries; // protected by Mutex

    const TDelayParams DelayParams;
    const NHdrf::NSnapshot::ELeafFairShare FairShareMode;
    TIntrusivePtr<TKqpCounters> KqpCounters;

    struct {
        NMonitoring::TDynamicCounters::TCounterPtr UpdateFairShare;
    } Counters;
};

using TComputeSchedulerPtr = std::shared_ptr<TComputeScheduler>;

struct TOptions {
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

        // Because datashard may get EvRead from another node, it's hard to use EvAddQuery+EvQueryResponse.
        // These messages return factory that can create schedulable objects on-the-fly.
        EvGetReadFactory,
        EvReadFactoryResponse,
    };
};

struct TEvAddDatabase : public TEventLocal<TEvAddDatabase, TEvents::EvAddDatabase> {
    explicit TEvAddDatabase(const TString& databaseId) : DatabaseId(databaseId) {}

    TString DatabaseId;
    double Weight = 1.;
};

struct TEvRemoveDatabase : public TEventLocal<TEvRemoveDatabase, TEvents::EvRemoveDatabase> {
    TString DatabaseId;
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
    NHdrf::TQueryId QueryId;
};

struct TEvQueryResponse : public TEventLocal<TEvQueryResponse, TEvents::EvQueryResponse> {
    NHdrf::NDynamic::TQueryPtr Query;
};

struct TEvGetReadFactory : public TEventLocal<TEvGetReadFactory, TEvents::EvGetReadFactory> {
    // TODO: datashard id?
};

struct TEvReadFactoryResponse : public TEventLocal<TEvReadFactoryResponse, TEvents::EvReadFactoryResponse> {
    TSchedulableReadFactoryPtr Factory;
};

} // namespace NKikimr::NKqp::NScheduler

namespace NKikimr::NKqp {
    IActor* CreateKqpComputeSchedulerService(const NScheduler::TOptions& options);
}

#pragma once

#include "fwd.h"
#include "tree/static.h"

#include <ydb/core/base/events.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/mutex.h>
#include <util/system/rwlock.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>

namespace NKikimr::NKqp::NScheduler {

class TCpuGuaranteeError : public yexception {};

class TComputeScheduler : public std::enable_shared_from_this<TComputeScheduler> {
public:
    TComputeScheduler(const NMonitoring::TDynamicCounterPtr& counters, const TOptions& options);

    void ToggleEnabled(bool enable) {
        Enabled = enable;
    }
    bool IsEnabled() const {
        return Enabled;
    }

    bool IsMemoryEnabled() const {
        return MemoryEnabled;
    }

    // Consumes exclusive ownership, validates and publishes the tree as const.
    // Builder references must not be used after the transfer. Runtime accounting is preserved.
    // False means multiple databases disable effective guarantees.
    bool UpdateConfiguration(std::unique_ptr<NHdrf::NStatic::TRoot> tree);
    NHdrf::NStatic::TRootPtr GetStaticTree() const;

    ui64 GetTotalCpuLimit() const;

    // The memory of all the queries on the node never exceeds it
    ui64 GetTotalMemoryLimit() const;

    // The memory of all the queries on the node.
    ui64 GetTotalMemoryUsage() const;
    // The expected memory of all the queries on the node: the initial (M) and the elastic (E) memory of their tasks.
    ui64 GetTotalMemoryDemand() const;

    // The pool which accounts the memory of the queries of the given pool - the memory of any query is accounted,
    // so the database and the pool are created if needed. An empty pool means the default one.
    NHdrf::NDynamic::TPoolPtr GetOrCreateMemoryPool(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId);

    NHdrf::NDynamic::TQueryPtr AddOrUpdateQuery(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId, const NHdrf::TQueryId& queryId);
    NHdrf::NDynamic::TQueryPtr GetReadQuery(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId);
    bool RemoveQuery(const NHdrf::TQueryId& queryId, bool isForceRemove = false);

    void UpdateFairShare();

    // Returns per-leaf-pool CpuFairShare / TotalCpu, normalized to [0..1].
    THashMap<NHdrf::TFullPoolId, double> GetLeafPoolFairShares() const;

private:
    NHdrf::NDynamic::TDatabasePtr GetOrCreateDatabase(const NHdrf::TDatabaseId& databaseId); // run under Mutex

    NHdrf::NDynamic::TPoolPtr CreatePool(const NHdrf::NDynamic::TPoolPtr& parent, const NHdrf::TPoolId& poolId); // run under Mutex
    NHdrf::NDynamic::TQueryPtr EnsureReadQuery(const NHdrf::NDynamic::TPoolPtr& pool, const NHdrf::TFullPoolId& poolId); // run under Mutex

private:

    static constexpr NHdrf::TQueryId READ_QUERY_ID = -1;

    std::atomic<bool> Enabled;
    const bool MemoryEnabled;

    TRWMutex Mutex;
    TMutex UpdateMutex; // serializes snapshot accounting and publication
    struct TQueryState {
        ui64 AddQueryCount;
        NHdrf::NDynamic::TQueryPtr Query;
    };
    NHdrf::NDynamic::TRootPtr Root;                 // protected by Mutex
    THashMap<NHdrf::TQueryId, TQueryState> Queries; // protected by Mutex

    // Special virtual queries per each pool to create SchedulableRead upon them, used for datashards and columnshards.
    // TODO: get rid of read queries - just pass somehow the real query to datashards.
    THashMap<NHdrf::TFullPoolId, NHdrf::NDynamic::TQueryPtr> ReadQueries; // protected by Mutex

    const TDelayParams DelayParams;
    NMonitoring::TDynamicCounterPtr KqpCounters;

    struct {
        NMonitoring::TDynamicCounters::TCounterPtr UpdateFairShare;
    } Counters;
};

using TComputeSchedulerPtr = std::shared_ptr<TComputeScheduler>;

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
};

struct TEvRemoveQuery : public TEventLocal<TEvRemoveQuery, TEvents::EvRemoveQuery> {
    NHdrf::TQueryId QueryId;
    bool IsForceRemove = false;
};

struct TEvQueryResponse : public TEventLocal<TEvQueryResponse, TEvents::EvQueryResponse> {
    NHdrf::NDynamic::TQueryPtr Query;
};

} // namespace NKikimr::NKqp::NScheduler

namespace NKikimrConfig {
    class TAppConfig;
}

namespace NKikimr::NKqp {
    NScheduler::TComputeSchedulerPtr CreateKqpComputeScheduler(
        const NMonitoring::TDynamicCounterPtr& counters,
        const NKikimrConfig::TAppConfig& appConfig);
}

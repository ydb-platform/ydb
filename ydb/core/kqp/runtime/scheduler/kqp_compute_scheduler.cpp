#include "kqp_compute_scheduler_service.h"

#include "tree/dynamic.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>

#include <algorithm>
#include <cmath>

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NScheduler;
using namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic;

namespace {

bool IsValidWeight(double weight) {
    return std::isfinite(weight) && weight > 0.0;
}

void ValidateAttributes(const NHdrf::TStaticAttributes& attrs) {
    Y_ENSURE(IsValidWeight(attrs.GetWeight()), "Weight should be finite and positive");
    Y_ENSURE(attrs.GetReadLimit() <= TDuration::Seconds(1), "ReadLimit should not exceed one second");
    Y_ENSURE_EX(attrs.GetCpuGuarantee() <= attrs.GetCpuLimit(), TCpuGuaranteeError()
        << "CpuGuarantee exceeds CpuLimit");
}

// Only the candidate participates in validation; no old runtime attributes are consulted.
ui64 ValidateStaticPool(const NHdrf::NStatic::TPool& pool, ui64 budget, bool supportsGuarantees) {
    ValidateAttributes(pool);
    const ui64 limit = Min(budget, pool.GetCpuLimit());
    ui64 unreserved = Min(limit, pool.CpuGuarantee.value_or(limit));
    ui64 reserved = 0;
    pool.ForEachChild<NHdrf::NStatic::TPool>([&](const NHdrf::NStatic::TPool* child, size_t) {
        Y_ENSURE(!std::get<NHdrf::TPoolId>(child->GetId()).empty(), "Pool id should not be empty");
        const ui64 guarantee = ValidateStaticPool(*child, limit, supportsGuarantees);
        if (supportsGuarantees) {
            Y_ENSURE_EX(guarantee <= unreserved, TCpuGuaranteeError()
                << "Children's CpuGuarantee exceeds the parent's budget");
            unreserved -= guarantee;
            reserved += guarantee;
        }
    });
    const ui64 guarantee = pool.IsLeaf() ? pool.GetCpuGuarantee() : reserved;
    Y_ENSURE_EX(!supportsGuarantees || guarantee <= limit, TCpuGuaranteeError()
        << "CpuGuarantee exceeds the available CPU budget");
    return guarantee;
}

} // namespace

namespace NKikimr::NKqp {

namespace NScheduler {

TComputeScheduler::TComputeScheduler(const NMonitoring::TDynamicCounterPtr& counters, const TOptions& options)
    : Enabled(options.Enabled)
    , MemoryEnabled(options.Enabled && options.MemoryEnabled)
    , Root(std::make_shared<TRoot>(counters))
    , DelayParams(options.DelayParams)
    , KqpCounters(counters)
{
    auto group = counters;
    Counters.UpdateFairShare = group->GetCounter("scheduler/UpdateFairShare", true);
}

bool TComputeScheduler::UpdateConfiguration(std::unique_ptr<NHdrf::NStatic::TRoot> tree) {
    Y_ENSURE(tree, "Static tree is required");
    NHdrf::NStatic::TRootPtr candidate(std::move(tree));
    const bool supportsGuarantees = candidate->SupportsGuarantees();
    candidate->ForEachChild<NHdrf::NStatic::TDatabase>([&](const NHdrf::NStatic::TDatabase* database, size_t) {
        // Registration addresses a pool by (databaseId, poolId), not by a nested path.
        // Reject unsupported topology before publishing it over any live queries.
        database->ForEachChild<NHdrf::NStatic::TPool>([](const NHdrf::NStatic::TPool* pool, size_t) {
            Y_ENSURE(pool->IsLeaf(), "Nested scheduler pools are not supported: " << pool->GetId());
        });
        ValidateStaticPool(*database, candidate->TotalCpuLimit, supportsGuarantees);
    });

    TGuard updateLock(UpdateMutex);
    TWriteGuard lock(Mutex);
    // Runtime paths are created lazily by registration, never rebuilt on a policy change.
    Root->SetStaticTree(std::move(candidate));
    return supportsGuarantees;
}

NHdrf::NStatic::TRootPtr TComputeScheduler::GetStaticTree() const {
    return Root->GetStaticTree();
}

ui64 TComputeScheduler::GetTotalCpuLimit() const {
    return GetStaticTree()->TotalCpuLimit;
}

ui64 TComputeScheduler::GetTotalMemoryLimit() const {
    return GetStaticTree()->TotalMemoryLimit;
}

ui64 TComputeScheduler::GetTotalMemoryUsage() const {
    return Root->MemoryUsage.load(std::memory_order_relaxed);
}

ui64 TComputeScheduler::GetTotalMemoryDemand() const {
    return Root->MemoryDemand.load(std::memory_order_relaxed);
}

TPoolPtr TComputeScheduler::GetOrCreateMemoryPool(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId) {
    const TString memoryPoolId = poolId.empty() ? TString(NResourcePool::DEFAULT_POOL_ID) : poolId;

    {
        TReadGuard lock(Mutex);
        if (auto database = Root->GetDatabase(databaseId)) {
            if (auto existingPool = database->GetPool(memoryPoolId)) {
                return std::static_pointer_cast<TPool>(existingPool);
            }
        }
    }

    TWriteGuard lock(Mutex);
    auto database = GetOrCreateDatabase(databaseId);
    if (auto existingPool = database->GetPool(memoryPoolId)) {
        return std::static_pointer_cast<TPool>(existingPool);
    }

    // The limits of the pool come later, with its configuration - see TComputeSchedulerService::ApplyPoolConfig
    return CreatePool(database, memoryPoolId);
}

NHdrf::NDynamic::TDatabasePtr TComputeScheduler::GetOrCreateDatabase(const NHdrf::TDatabaseId& databaseId) {
    if (auto database = Root->GetDatabase(databaseId)) {
        return database;
    }

    auto database = std::make_shared<TDatabase>(databaseId);
    Root->AddDatabase(database);
    return database;
}

TPoolPtr TComputeScheduler::CreatePool(const TPoolPtr& parent, const NHdrf::TPoolId& poolId) {
    TPoolPtr pool;
    if (poolId == NResourcePool::DEFAULT_POOL_ID) {
        pool = std::make_shared<TDefaultPool>(poolId, KqpCounters);
    } else {
        pool = std::make_shared<TPool>(poolId, KqpCounters);
    }
    parent->AddPool(pool);
    return pool;
}

TQueryPtr TComputeScheduler::EnsureReadQuery(const TPoolPtr& pool, const NHdrf::TFullPoolId& poolId) {
    if (const auto it = ReadQueries.find(poolId); it != ReadQueries.end()) {
        return it->second;
    }
    auto query = std::make_shared<TQuery>(READ_QUERY_ID, &DelayParams, false);
    pool->AddQuery(query);
    // Use the same initial pool shares as ordinary memory-enabled queries.
    if (const auto snapshot = MemoryEnabled ? Root->GetSnapshot() : nullptr) {
        if (const auto databaseSnapshot = snapshot->GetDatabase(poolId.DatabaseId)) {
            if (const auto poolSnapshot = databaseSnapshot->GetPool(poolId.PoolId)) {
                query->InitSnapshot(*std::static_pointer_cast<NHdrf::NSnapshot::TPool>(poolSnapshot));
            }
        }
    }
    ReadQueries.emplace(poolId, query);
    return query;
}

TQueryPtr TComputeScheduler::AddOrUpdateQuery(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId, const NHdrf::TQueryId& queryId) {
    Y_ENSURE(!poolId.empty());

    TWriteGuard lock(Mutex);
    Y_ENSURE(queryId != READ_QUERY_ID, "Query id is reserved for reads");

    TQueryPtr query;
    if (auto it = Queries.find(queryId); it != Queries.end()) {
        auto& state = it->second;
        const auto fullPoolId = state.Query->GetFullPoolId();
        Y_ENSURE(fullPoolId.DatabaseId == databaseId && fullPoolId.PoolId == poolId,
            "Query is already registered in a different pool: " << queryId);
        ++state.AddQueryCount;
        query = state.Query;
    } else {
        const auto tree = GetStaticTree();
        const auto configuredPool = NHdrf::NStatic::GetPool(NHdrf::NStatic::GetDatabase(tree, databaseId), poolId);
        auto database = Root->GetDatabase(databaseId);
        auto pool = database ? std::static_pointer_cast<TPool>(database->GetPool(poolId)) : nullptr;
        Y_ENSURE(pool || configuredPool, "Pool not configured: " << databaseId << "/" << poolId);
        Y_ENSURE(!configuredPool || configuredPool->IsLeaf(), "Queries require a leaf pool");
        if (!pool) {
            pool = CreatePool(GetOrCreateDatabase(databaseId), poolId);
        }
        EnsureReadQuery(pool, {databaseId, poolId});
        query = std::make_shared<TQuery>(queryId, &DelayParams, true);
        pool->AddQuery(query);
        Y_ENSURE(Queries.emplace(queryId, TQueryState{1, query}).second);
    }

    // Reuse the pool's shares for subsequent queries without recalculating the whole tree.
    if (const auto snapshot = Root->GetSnapshot()) {
        if (const auto databaseSnapshot = snapshot->GetDatabase(databaseId)) {
            if (const auto poolSnapshot = databaseSnapshot->GetPool(poolId)) {
                if (MemoryEnabled && !query->GetSnapshot()) {
                    query->InitSnapshot(*std::static_pointer_cast<NHdrf::NSnapshot::TPool>(poolSnapshot));
                }
                return query;
            }
        }
    }

    // A read or memory user may have created the runtime pool before this registration.
    // Publish its first snapshot before QueryResponse lets the caller allocate task memory.
    lock.Release();
    UpdateFairShare();
    return query;
}

NHdrf::NDynamic::TQueryPtr TComputeScheduler::GetReadQuery(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId) {
    if (!IsEnabled()) {
        return {};
    }

    const NHdrf::TFullPoolId fullPoolId{databaseId, poolId};
    {
        TReadGuard lock(Mutex);
        if (auto it = ReadQueries.find(fullPoolId); it != ReadQueries.end()) {
            return it->second;
        }
    }

    TWriteGuard lock(Mutex);
    if (auto it = ReadQueries.find(fullPoolId); it != ReadQueries.end()) {
        return it->second;
    }

    const auto tree = GetStaticTree();
    const auto configuredPool = NHdrf::NStatic::GetPool(NHdrf::NStatic::GetDatabase(tree, databaseId), poolId);
    if (!configuredPool || !configuredPool->IsLeaf()) {
        return {};
    }
    auto parent = GetOrCreateDatabase(databaseId);
    auto pool = std::static_pointer_cast<TPool>(parent->GetPool(poolId));
    if (!pool) {
        pool = CreatePool(parent, poolId);
    }
    return EnsureReadQuery(pool, fullPoolId);
}

bool TComputeScheduler::RemoveQuery(const NHdrf::TQueryId& queryId, const bool isForceRemove) {
    TWriteGuard lock(Mutex);

    if (auto queryIt = Queries.find(queryId); queryIt != Queries.end()) {
        auto& state = queryIt->second;
        Y_ENSURE(state.AddQueryCount > 0, "Query has no registrations: " << queryId);
        if (isForceRemove || --state.AddQueryCount == 0) {
            state.Query->GetParent()->RemoveQuery(queryId);
            Queries.erase(queryIt);
        }
        return true;
    }

    return false;
}

THashMap<NHdrf::TFullPoolId, double> TComputeScheduler::GetLeafPoolFairShares() const {
    THashMap<NHdrf::TFullPoolId, double> result;
    auto snapshot = Root->GetSnapshot();
    if (!snapshot) {
        return result;
    }
    const auto totalCpu = snapshot->TotalCpuLimit;
    if (!totalCpu) {
        return result;
    }

    // The pools created only to account the memory (see GetOrCreateMemoryPool) are not scheduled by CPU.
    TReadGuard lock(Mutex);

    auto visitPool = [&](auto self, const NHdrf::TDatabaseId& databaseId, const auto* pool) -> void {
        if (pool->IsLeaf()) {
            NHdrf::TFullPoolId fullPoolId{databaseId, std::get<NHdrf::TPoolId>(pool->GetId())};
            if (!ReadQueries.contains(fullPoolId)) {
                return;
            }
            result[fullPoolId] = double(pool->CpuFairShare) / totalCpu;
        } else {
            pool->template ForEachChild<NHdrf::NSnapshot::TPool>([&](auto* child, size_t) {
                self(self, databaseId, child);
            });
        }
    };

    snapshot->ForEachChild<NHdrf::NSnapshot::TDatabase>([&](auto* database, size_t) {
        const auto& databaseId = std::get<NHdrf::TDatabaseId>(database->GetId());
        database->template ForEachChild<NHdrf::NSnapshot::TPool>([&](auto* pool, size_t) {
            visitPool(visitPool, databaseId, pool);
        });
    });
    return result;
}

void TComputeScheduler::UpdateFairShare() {
    TGuard updateLock(UpdateMutex);
    auto startTime = TMonotonic::Now();

    NHdrf::NSnapshot::TRootPtr snapshot;
    {
        TWriteGuard lock(Mutex);
        snapshot = NHdrf::NSnapshot::TRootPtr(Root->TakeSnapshot());
    }

    snapshot->Update(Root->GetSnapshot());

    {
        TWriteGuard lock(Mutex);
        // Never publish effective values for an obsolete configuration after a replacement.
        if (snapshot->StaticTree == Root->GetStaticTree()) {
            Root->SetSnapshot(snapshot);
        }
    }

    Counters.UpdateFairShare->Add((TMonotonic::Now() - startTime).MicroSeconds());
}

} // namespace NScheduler

NScheduler::TComputeSchedulerPtr CreateKqpComputeScheduler(const NMonitoring::TDynamicCounterPtr& counters, const NKikimrConfig::TAppConfig& appConfig) {
    const auto& schedulerSettings = appConfig.GetTableServiceConfig().GetComputeSchedulerSettings();

    auto options = TOptions{
        .Enabled = appConfig.GetFeatureFlags().GetEnableResourcePoolsScheduler(),
        .DelayParams = TDelayParams{
            .MaxDelay = TDuration::MicroSeconds(schedulerSettings.GetMaxTaskDelayUs()),
            .MinDelay = TDuration::MicroSeconds(schedulerSettings.GetMinTaskDelayUs()),
            .AttemptBonus = TDuration::MicroSeconds(schedulerSettings.GetAttemptTaskBonusUs()),
            .MaxRandomDelay = TDuration::MicroSeconds(schedulerSettings.GetMaxTaskRandomDelayUs()),
        },
        // the resource manager decides the same from the app data, see TComputeScheduler::IsMemoryEnabled
        .MemoryEnabled = appConfig.GetFeatureFlags().GetEnableComputeSchedulerMemory(),
    };

    Y_ENSURE(options.DelayParams.MaxDelay > TDuration::Zero());
    Y_ENSURE(options.DelayParams.MinDelay > TDuration::Zero());
    Y_ENSURE(options.DelayParams.AttemptBonus > TDuration::Zero());
    Y_ENSURE(options.DelayParams.MaxRandomDelay > TDuration::Zero());
    auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(GetServiceCounters(counters, "kqp"), options);

    // The initial memory limit - until the one from the memory controller comes.
    if (options.MemoryEnabled) {
        const auto& resourceManagerConfig = appConfig.GetTableServiceConfig().GetResourceManager();
        scheduler->UpdateConfiguration(std::make_unique<NScheduler::NHdrf::NStatic::TRoot>(
            NScheduler::NHdrf::Infinity(), resourceManagerConfig.GetQueryMemoryLimit()));
    }

    return scheduler;
}

} // namespace NKikimr::NKqp

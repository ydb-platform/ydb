#include "kqp_compute_scheduler_service.h"

#include "tree/dynamic.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/subsystems/stats.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_COMPUTE_SCHEDULER

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NScheduler;
using namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic;

namespace {

using TDynamicElement = NHdrf::TTreeElementBase<NHdrf::ETreeType::DYNAMIC>;

// Validates the final (merged) configuration which is about to be applied to a tree element.
// `element` is the element itself when it already exists, `parent` is its parent-to-be - passing
// no parent means that the element is not validated against the parent's guarantee at all.
void ValidateAttributes(const NHdrf::TStaticAttributes& attrs, const TDynamicElement* element, const TDynamicElement* parent) {
    // Validate weight
    Y_ENSURE(attrs.GetWeight() > 0.0, "Weight should be positive");

    // Validate guarantee
    if (attrs.CpuGuarantee) {
        const auto guarantee = attrs.GetCpuGuarantee();

        Y_ENSURE_EX(guarantee <= attrs.GetCpuLimit(), TCpuGuaranteeError()
            << "CpuGuarantee (" << guarantee << ") should not exceed CpuLimit (" << attrs.GetCpuLimit() << ")");

        // A zero guarantee reserves nothing from the parent - resetting is always allowed
        if (parent && guarantee > 0) {
            Y_ENSURE_EX(parent->CpuGuarantee, TCpuGuaranteeError()
                << "Child cannot set CpuGuarantee until the parent's guarantee is set");

            // Calculate unreserved parent guarantee excluding `element`
            ui64 unreserved = *parent->CpuGuarantee;

            parent->ForEachChild<TDynamicElement>([&](const TDynamicElement* child, size_t) {
                if (child != element) {
                    // TODO: replace with std::sub_sat() in C++26
                    const auto guarantee = child->GetCpuGuarantee();
                    unreserved = unreserved > guarantee ? unreserved - guarantee : 0;
                }
            });

            Y_ENSURE_EX(guarantee <= unreserved, TCpuGuaranteeError()
                << "CpuGuarantee (" << guarantee << ") exceeds the guarantee left by the parent (" << unreserved << ")");
        }

        if (element) {
            const auto reserved = element->GetChildrenCpuGuarantee();
            Y_ENSURE_EX(guarantee >= reserved, TCpuGuaranteeError()
                << "CpuGuarantee (" << guarantee << ") is less than the guarantees already reserved by children (" << reserved << ")");
        }
    }
}

class TComputeSchedulerService : public NActors::TActorBootstrapped<TComputeSchedulerService> {
public:
    explicit TComputeSchedulerService(TDuration updateFairSharePeriod) : UpdateFairSharePeriod(updateFairSharePeriod) {}

    void Bootstrap() {
        Scheduler = AppData()->KqpComputeScheduler;
        Y_ENSURE(Scheduler);

        Send(
            NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({(ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem}),
            NActors::IEventHandle::FlagTrackDelivery
        );

        if (Scheduler->IsEnabled()) {
            YDB_LOG_INFO("Enabled on start");
        } else {
            YDB_LOG_INFO("Disabled on start");
        }

        Scheduler->SetTotalCpuLimit(CalculateTotalCpuLimit()); // TODO: take total cpu limit from outside

        // The node's own database is known before the scheduler may be used by anyone - the state is
        // not entered yet, so no event can be handled before it is registered. The other databases
        // (serverless) are registered by the proxy service once they are resolved.
        if (const auto& tenantName = AppData()->TenantName; !tenantName.empty()) {
            Scheduler->AddOrUpdateDatabase(CanonizePath(tenantName), {});
        }

        Send(NKikimr::NMemory::MakeMemoryControllerId(), new NKikimr::NMemory::TEvConsumerRegister(NKikimr::NMemory::EMemoryConsumerKind::QueryExecution));

        Become(&TComputeSchedulerService::State);
        Schedule(UpdateFairSharePeriod, new NActors::TEvents::TEvWakeup());
    }

    STATEFN(State) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);

            hFunc(TEvAddDatabase, Handle);
            hFunc(TEvRemoveDatabase, Handle);
            hFunc(TEvAddPool, Handle);
            hFunc(NWorkloadManager::TEvUpdatePoolInfo, Handle);
            hFunc(TEvRemovePool, Handle);
            hFunc(TEvAddQuery, Handle);
            hFunc(TEvRemoveQuery, Handle);

            hFunc(NActors::TEvents::TEvWakeup, Handle);

            hFunc(NKikimr::NMemory::TEvConsumerRegistered, Handle);
            hFunc(NKikimr::NMemory::TEvConsumerLimit, Handle);

            default:
                YDB_LOG_ERROR("Unexpected",
                    {"event", ev->GetTypeRewrite()});
        }
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        YDB_LOG_DEBUG("Subscribed to config changes");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& event = ev->Get()->Record;

        // Only the CPU is scheduled optionally - the memory is always limited by the scheduler.
        Scheduler->ToggleEnabled(event.GetConfig().GetFeatureFlags().GetEnableResourcePoolsScheduler());
        if (Scheduler->IsEnabled()) {
            YDB_LOG_INFO("Become enabled");
        } else {
            YDB_LOG_INFO("Become disabled");
        }

        auto responseEvent = std::make_unique<NKikimr::NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), NActors::IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvAddDatabase::TPtr& ev) {
        NHdrf::TStaticAttributes const attrs {
            .Weight = ev->Get()->Weight, // TODO: weight shouldn't be negative!
        };
        Scheduler->AddOrUpdateDatabase(ev->Get()->DatabaseId, attrs);

        YDB_LOG_DEBUG("Add",
            {"database", ev->Get()->DatabaseId},
            {"attrs", attrs});
    }

    void Handle(TEvRemoveDatabase::TPtr&) {
        Y_ABORT("Unsupported yet");
    }

    void Handle(TEvAddPool::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId;
        NHdrf::TStaticAttributes attrs = {
            .Weight = ev->Get()->Weight, // TODO: weight shouldn't be negative!
        };

        Y_ASSERT(!poolId.empty());

        SetCpuAttributes(ev->Get()->Params, attrs);
        SetMemoryAttributes({databaseId, poolId}, ev->Get()->Params, attrs);

        YDB_LOG_DEBUG("Add",
            {"pool", databaseId},
            {"poolId", poolId},
            {"attrs", attrs});

        if (PoolSubscribtions.emplace(NHdrf::TFullPoolId{.DatabaseId=databaseId, .PoolId=poolId}, false).second) {
            ApplyPoolConfig(databaseId, poolId, attrs);
            Send(NWorkloadManager::MakeServiceId(SelfId().NodeId()), new NWorkloadManager::TEvSubscribeOnPoolChanges(databaseId, poolId));
        }
    }

    void Handle(TEvRemovePool::TPtr&) {
        Y_ABORT("Unsupported yet");
    }

    void Handle(NWorkloadManager::TEvUpdatePoolInfo::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId;
        auto poolIt = PoolSubscribtions.find(NHdrf::TFullPoolId{databaseId, poolId});

        if (ev->Get()->Config) {
            Y_ENSURE(poolIt != PoolSubscribtions.end());
            poolIt->second = false;

            NHdrf::TStaticAttributes attrs;
            SetCpuAttributes(*ev->Get()->Config, attrs);
            SetMemoryAttributes(poolIt->first, *ev->Get()->Config, attrs);
            ApplyPoolConfig(databaseId, poolId, attrs);

            YDB_LOG_DEBUG("Update",
                {"pool", databaseId},
                {"poolId", poolId},
                {"attrs", attrs});
        } else if (poolIt != PoolSubscribtions.end()) {
            if (!poolIt->second) {
                // The first removal - try to re-subscribe in case it's just the pool removal from cache.
                poolIt->second = true;
                Send(NWorkloadManager::MakeServiceId(SelfId().NodeId()), new NWorkloadManager::TEvSubscribeOnPoolChanges(databaseId, poolId));
            } else {
                // The second removal - the pool was really removed.
                PoolSubscribtions.erase(poolIt);
                // TODO: Scheduler->RemovePool(…);
                // TODO: Scheduler->UpdatePool(…);
            }
        } else {
            YDB_LOG_ERROR("Trying to remove unknown",
                {"pool", databaseId},
                {"poolId", poolId});
            // TODO: the removing message for unknown pool - should we check?
        }
    }

    void Handle(TEvAddQuery::TPtr& ev) {
        const auto& databaseId = ev->Get()->DatabaseId;
        const auto& poolId = ev->Get()->PoolId;
        const auto& queryId = ev->Get()->QueryId;
        NHdrf::TStaticAttributes const attrs {
            .Weight = ev->Get()->Weight, // TODO: weight shouldn't be negative!
        };

        // The query is always added - its memory is accounted anyway, and the CPU is scheduled only when the scheduler
        // is enabled (see the compute actor factory)
        auto response = MakeHolder<TEvQueryResponse>();
        response->Query = Scheduler->AddOrUpdateQuery(databaseId, poolId.empty() ? NKikimr::NResourcePool::DEFAULT_POOL_ID : poolId, queryId, attrs);
        YDB_LOG_DEBUG("Add",
            {"query", databaseId},
            {"poolId", poolId},
            {"txId", queryId});
        Send(ev->Sender, response.Release(), 0, queryId);
    }

    void Handle(TEvRemoveQuery::TPtr& ev) {
        const auto& queryId = ev->Get()->QueryId;
        const bool isForceRemove = ev->Get()->IsForceRemove;
        if (!Scheduler->RemoveQuery(queryId, isForceRemove)) {
            YDB_LOG_ERROR("Trying to remove unknown query",
                {"queryId", queryId},
                {"isForceRemove", isForceRemove});
        } else {
            YDB_LOG_DEBUG("Remove query",
                {"txId", queryId});
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        Scheduler->UpdateFairShare();
        ReportMemoryConsumption();
        PublishMemoryState();
        Schedule(UpdateFairSharePeriod, new NActors::TEvents::TEvWakeup());
    }

    void Handle(NKikimr::NMemory::TEvConsumerRegistered::TPtr& ev) {
        MemoryConsumer = std::move(ev->Get()->Consumer);
        ReportMemoryConsumption();

        YDB_LOG_INFO("Registered as memory consumer");
    }

    void Handle(NKikimr::NMemory::TEvConsumerLimit::TPtr& ev) {
        const ui64 limit = ev->Get()->LimitBytes;
        if (limit == Scheduler->GetTotalMemoryLimit()) {
            return;
        }

        YDB_LOG_INFO("Total memory limit",
            {"limit", limit},
            {"previous", Scheduler->GetTotalMemoryLimit()});

        Scheduler->SetTotalMemoryLimit(limit);

        // The memory limits of the pools are the shares of the total limit - they follow it.
        for (const auto& [fullPoolId, memoryLimitPercent] : PoolMemoryLimitPercents) {
            NHdrf::TStaticAttributes attrs;
            attrs.MemoryLimit = ToMemoryLimit(memoryLimitPercent);
            try {
                Scheduler->AddOrUpdatePool(fullPoolId.DatabaseId, fullPoolId.PoolId, attrs);
            } catch (const TCpuGuaranteeError& e) {
                YDB_LOG_ERROR("Failed to update the memory limit",
                    {"pool", fullPoolId.DatabaseId},
                    {"poolId", fullPoolId.PoolId},
                    {"error", TString(e.what())});
            }
        }
    }

private:
    ui64 CalculateTotalCpuLimit() {
        auto poolId = SelfId().PoolID();
        NActors::TExecutorPoolStats poolStats;
        TVector<NActors::TExecutorThreadStats> threadsStats;
        NActors::GetActorSystemStats().GetPoolStats(poolId, poolStats, threadsStats);
        return Max<ui64>(poolStats.MaxThreadCount, 1);
    }

    // Update limit and guarantee - they are applied together,
    // since lowering the limit of a pool has to lower its guarantee as well.
    // A limit percent of -1 means that the setting is not configured, so the previous value is kept.
    // The guarantee is different: the config always carries the whole pool description, so -1 means
    // that the pool has no guarantee anymore and the reservation it holds has to be released.
    void SetCpuAttributes(const NResourcePool::TPoolSettings& config, NHdrf::TStaticAttributes& attrs) const {
        const auto totalCpuLimit = Scheduler->GetTotalCpuLimit();

        if (const auto& cpuLimitPercent = config.TotalCpuLimitPercentPerNode; cpuLimitPercent >= 0) {
            if (cpuLimitPercent == 0) {
                attrs.CpuLimit = 0;
                attrs.ReadLimit = TDuration::Zero();
            } else {
                attrs.CpuLimit = std::max<ui64>(1, cpuLimitPercent * totalCpuLimit / 100);
                attrs.ReadLimit = TDuration::MilliSeconds(cpuLimitPercent * 10);
            }
        }

        attrs.CpuGuarantee = static_cast<ui64>(std::max(config.TotalCpuGuaranteePercentPerNode, 0.0) * totalCpuLimit / 100);
    }

    // A limit percent of -1 means that the setting is not configured, so the previous value is kept.
    // The percent is remembered, since the limit follows the total memory limit.
    void SetMemoryAttributes(const NHdrf::TFullPoolId& fullPoolId, const NResourcePool::TPoolSettings& config, NHdrf::TStaticAttributes& attrs) {
        if (const auto& memoryLimitPercent = config.TotalMemoryLimitPercentPerNode; memoryLimitPercent >= 0) {
            PoolMemoryLimitPercents[fullPoolId] = memoryLimitPercent;
            attrs.MemoryLimit = ToMemoryLimit(memoryLimitPercent);
        }
    }

    ui64 ToMemoryLimit(double memoryLimitPercent) const {
        const auto totalMemoryLimit = Scheduler->GetTotalMemoryLimit();
        return totalMemoryLimit == NHdrf::Infinity()
            ? NHdrf::Infinity()
            : static_cast<ui64>(std::clamp(memoryLimitPercent, 0.0, 100.0) / 100 * totalMemoryLimit);
    }

    // The demand is the expected memory of the queries - but never less than the one they already use.
    void ReportMemoryConsumption() {
        if (MemoryConsumer) {
            const ui64 usage = Scheduler->GetTotalMemoryUsage();
            MemoryConsumer->SetReport({
                .Used = usage,
                .Demand = Max(usage, Scheduler->GetTotalMemoryDemand()),
            });
        }
    }

    // The resource manager publishes the memory of the queries to the other nodes
    void PublishMemoryState() {
        const ui64 limit = Scheduler->GetTotalMemoryLimit();
        const ui64 usage = Scheduler->GetTotalMemoryUsage();
        if (limit != PublishedMemoryLimit || usage != PublishedMemoryUsage) {
            Send(MakeKqpRmServiceID(SelfId().NodeId()), new NRm::TEvQueryMemoryState(limit, usage));
            PublishedMemoryLimit = limit;
            PublishedMemoryUsage = usage;
        }
    }

    // TODO: handle invalid configuration on DDL level.
    // TODO: retry the rejected configuration once the sibling pools release their guarantees.
    //       Every pool is watched by its own handler actor, so there is no ordering between the
    //       updates of different pools: redistributing the guarantees between two pools by two
    //       valid DDL queries may be delivered here in the reverse order. The pool that arrives
    //       first is then rejected against the stale guarantee of its sibling and stays without
    //       any guarantee until its own configuration changes again.
    void ApplyPoolConfig(const TString& databaseId, const TString& poolId, NHdrf::TStaticAttributes attrs) {
        try {
            Scheduler->AddOrUpdatePool(databaseId, poolId, attrs);
            return;
        } catch (const TCpuGuaranteeError& e) {
            YDB_LOG_ERROR("Rejected guarantee",
                {"pool", databaseId},
                {"poolId", poolId},
                {"attrs", attrs},
                {"error", TString(e.what())});
        }

        attrs.CpuGuarantee = 0;
        Scheduler->AddOrUpdatePool(databaseId, poolId, attrs);
    }

private:
    TComputeSchedulerPtr Scheduler;
    TIntrusivePtr<NKikimr::NMemory::IMemoryConsumer> MemoryConsumer;
    const TDuration UpdateFairSharePeriod;
    THashMap<NHdrf::TFullPoolId, bool /* IsFirstRemoval */> PoolSubscribtions;
    THashMap<NHdrf::TFullPoolId, double> PoolMemoryLimitPercents;
    ui64 PublishedMemoryLimit = 0;
    ui64 PublishedMemoryUsage = 0;
};

} // namespace

namespace NKikimr::NKqp {

namespace NScheduler {

TComputeScheduler::TComputeScheduler(const TIntrusivePtr<TKqpCounters>& counters, const TOptions& options)
    : Enabled(options.Enabled)
    , Root(std::make_shared<TRoot>(counters))
    , DelayParams(options.DelayParams)
    , KqpCounters(counters)
{
    auto group = counters->GetKqpCounters();
    Counters.UpdateFairShare = group->GetCounter("scheduler/UpdateFairShare", true);
}

// TODO: recalculate the guarantees of the whole tree here once the total limit becomes changeable.
//       A guarantee is converted from percents to cores when the configuration arrives and is kept
//       as an absolute value afterwards, so a changed total limit makes every one of them stale.
void TComputeScheduler::SetTotalCpuLimit(ui64 cpu) {
    Root->TotalCpuLimit = cpu;
    Root->CpuGuarantee = cpu;
}

ui64 TComputeScheduler::GetTotalCpuLimit() const {
    return Root->TotalCpuLimit.load();
}

void TComputeScheduler::SetTotalMemoryLimit(ui64 bytes) {
    Root->TotalMemoryLimit.store(bytes);
}

ui64 TComputeScheduler::GetTotalMemoryLimit() const {
    return Root->TotalMemoryLimit.load();
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
    return CreatePool(database, memoryPoolId, {});
}

void TComputeScheduler::SetDefaultDatabaseGuarantee(NHdrf::TStaticAttributes& attrs) const {
    if (!attrs.CpuGuarantee) {
        attrs.CpuGuarantee = Min<ui64>(Root->TotalCpuLimit, attrs.GetCpuLimit());
    }
}

NHdrf::NDynamic::TDatabasePtr TComputeScheduler::GetOrCreateDatabase(const NHdrf::TDatabaseId& databaseId) {
    if (auto database = Root->GetDatabase(databaseId)) {
        return database;
    }

    NHdrf::TStaticAttributes attrs;
    SetDefaultDatabaseGuarantee(attrs);

    auto database = std::make_shared<TDatabase>(databaseId, attrs);
    Root->AddDatabase(database);
    return database;
}

TPoolPtr TComputeScheduler::CreatePool(const TDatabasePtr& database, const NHdrf::TPoolId& poolId, const NHdrf::TStaticAttributes& attrs) {
    TPoolPtr pool;
    if (poolId == NResourcePool::DEFAULT_POOL_ID) {
        pool = std::make_shared<TDefaultPool>(poolId, KqpCounters, attrs);
    } else {
        pool = std::make_shared<TPool>(poolId, KqpCounters, attrs);
    }
    database->AddPool(pool);
    return pool;
}

void TComputeScheduler::AddOrUpdateDatabase(const TString& databaseId, const NHdrf::TStaticAttributes& attrs) {
    TWriteGuard lock(Mutex);

    auto database = Root->GetDatabase(databaseId);
    auto merged = database ? database->MergedWith(attrs) : attrs;
    SetDefaultDatabaseGuarantee(merged);

    // Databases are intentionally not validated against the root's guarantee.
    ValidateAttributes(merged, database.get(), nullptr);

    if (database) {
        database->Update(merged);
    } else {
        Root->AddDatabase(std::make_shared<TDatabase>(databaseId, merged));
    }
}

void TComputeScheduler::AddOrUpdatePool(const TString& databaseId, const TString& poolId, const NHdrf::TStaticAttributes& attrs) {
    Y_ENSURE(!poolId.empty());

    TWriteGuard lock(Mutex);
    auto database = GetOrCreateDatabase(databaseId);

    auto pool = database->GetPool(poolId);
    ValidateAttributes(pool ? pool->MergedWith(attrs) : attrs, pool.get(), database.get());

    if (pool) {
        pool->Update(attrs);
    } else {
        pool = CreatePool(database, poolId, attrs);
    }

    // The pool may already exist without the read query - if it was created only to account the memory.
    if (!ReadQueries.contains(NHdrf::TFullPoolId{databaseId, poolId})) {
        bool allowMinFairShare = !pool->CpuLimit || *pool->CpuLimit > 0;

        // Since they are not visible by query id - use the same id for each pool
        auto query = std::make_shared<TQuery>(READ_QUERY_ID, &DelayParams, allowMinFairShare, NHdrf::TStaticAttributes());
        pool->AddQuery(query);

        // Add read query
        ReadQueries.emplace(NHdrf::TFullPoolId{databaseId, poolId}, query);
    }
}

TQueryPtr TComputeScheduler::AddOrUpdateQuery(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId, const NHdrf::TQueryId& queryId, const NHdrf::TStaticAttributes& attrs) {
    Y_ENSURE(!poolId.empty());

    TWriteGuard lock(Mutex);
    auto database = Root->GetDatabase(databaseId);
    Y_ENSURE(database, "Database not found: " << databaseId);
    auto pool = database->GetPool(poolId);
    Y_ENSURE(pool, "Pool not found: " << poolId);

    if (auto it = Queries.find(queryId); it != Queries.end()) {
        auto& state = it->second;
        const auto fullPoolId = state.Query->GetFullPoolId();
        Y_ENSURE(fullPoolId.DatabaseId == databaseId && fullPoolId.PoolId == poolId,
            "Query is already registered in a different pool: " << queryId);
        ValidateAttributes(state.Query->MergedWith(attrs), state.Query.get(), pool.get());
        state.Query->Update(attrs);
        ++state.AddQueryCount;
        return state.Query;
    }

    ValidateAttributes(attrs, nullptr, pool.get());
    bool allowMinFairShare = !pool->CpuLimit || *pool->CpuLimit > 0;
    auto query = std::make_shared<TQuery>(queryId, &DelayParams, allowMinFairShare, attrs);
    pool->AddQuery(query);
    Y_ENSURE(Queries.emplace(queryId, TQueryState{1, query}).second);

    // The fair-share of the pool is known from the latest snapshot - unless the pool is new itself.
    if (const auto snapshot = Root->GetSnapshot()) {
        if (const auto databaseSnapshot = snapshot->GetDatabase(databaseId)) {
            if (const auto poolSnapshot = databaseSnapshot->GetPool(poolId)) {
                query->InitSnapshot(*std::static_pointer_cast<NHdrf::NSnapshot::TPool>(poolSnapshot));
            }
        }
    }

    return query;
}

NHdrf::NDynamic::TQueryPtr TComputeScheduler::GetReadQuery(const NHdrf::TDatabaseId& databaseId, const NHdrf::TPoolId& poolId) const {
    if (!IsEnabled()) {
        return {};
    }

    TReadGuard lock(Mutex);

    if (auto queryIt = ReadQueries.find(NHdrf::TFullPoolId{databaseId, poolId}); queryIt != ReadQueries.end()) {
        return queryIt->second;
    }

    return {};
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
    const auto totalCpu = GetTotalCpuLimit();
    if (!totalCpu) {
        return result;
    }
    auto snapshot = Root->GetSnapshot();
    if (!snapshot) {
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
    auto startTime = TMonotonic::Now();

    NHdrf::NSnapshot::TRootPtr snapshot;
    {
        TReadGuard lock(Mutex);
        snapshot = NHdrf::NSnapshot::TRootPtr(Root->TakeSnapshot());
    }

    snapshot->Update(Root->GetSnapshot());

    {
        TWriteGuard lock(Mutex);
        Root->SetSnapshot(snapshot);
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
        }
    };

    Y_ENSURE(options.DelayParams.MaxDelay > TDuration::Zero());
    Y_ENSURE(options.DelayParams.MinDelay > TDuration::Zero());
    Y_ENSURE(options.DelayParams.AttemptBonus > TDuration::Zero());
    Y_ENSURE(options.DelayParams.MaxRandomDelay > TDuration::Zero());
    auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(MakeIntrusive<NKqp::TKqpCounters>(counters), options);

    // The initial memory limit - until the one from the memory controller comes.
    const auto& resourceManagerConfig = appConfig.GetTableServiceConfig().GetResourceManager();
    scheduler->SetTotalMemoryLimit(resourceManagerConfig.GetQueryMemoryLimit());

    return scheduler;
}

IActor* CreateKqpComputeSchedulerService(TDuration updateFairSharePeriod) {
    Y_ENSURE(updateFairSharePeriod > TDuration::Zero());
    return new TComputeSchedulerService(updateFairSharePeriod);
}

} // namespace NKikimr::NKqp

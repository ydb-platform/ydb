#include "dynamic.h"

#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_task.h>

namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic {

namespace {

// Resolve the exact node in one pinned policy version. If requested, also
// collect the CPU limit of the existing prefix even when the leaf is missing.
NStatic::TPoolPtr ResolveStaticPool(const TPool& pool, ui64* cpuLimit = nullptr) {
    if (const auto* parent = pool.GetParent()) {
        const auto node = NStatic::GetPool(ResolveStaticPool(*parent, cpuLimit), std::get<TPoolId>(pool.GetId()));
        if (cpuLimit && node) {
            *cpuLimit = Min(*cpuLimit, node->GetCpuLimit());
        }
        return node;
    }
    const auto* root = dynamic_cast<const TRoot*>(&pool);
    Y_ENSURE(root, "Pool is not attached to the scheduler root");
    const auto tree = root->GetStaticTree();
    if (cpuLimit) {
        *cpuLimit = Min(tree->TotalCpuLimit, tree->GetCpuLimit());
    }
    return tree;
}

} // namespace

///////////////////////////////////////////////////////////////////////////////
// TTreeElement
///////////////////////////////////////////////////////////////////////////////

TPool* TTreeElement::GetParent() const {
    return dynamic_cast<TPool*>(Parent);
}

///////////////////////////////////////////////////////////////////////////////
// TQuery
///////////////////////////////////////////////////////////////////////////////

TQuery::TQuery(const TQueryId& id, const TDelayParams* delayParams, bool initializeCpuDemand)
    : NHdrf::TTreeElementBase<ETreeType::DYNAMIC>(id)
    , TTreeElement(id)
    , NHdrf::TQuery<ETreeType::DYNAMIC>(id)
    , DelayParams(delayParams)
    , InitializeCpuDemand(initializeCpuDemand)
{
}

NSnapshot::TQuery* TQuery::TakeSnapshot() {
    auto* newQuery = new NSnapshot::TQuery(std::get<TQueryId>(GetId()), shared_from_this());

    newQuery->CpuMaxDemand = TakeCpuMaxDemand();

    // The actual demand is smoothed with the one of the previous snapshot.
    if (const auto prevQuery = GetSnapshot()) {
        newQuery->PrevRawCpuActualDemand = prevQuery->RawCpuActualDemand;
    }

    // Update previous burst values and pass difference to new snapshot - to calculate adjusted satisfaction and actual demand
    const auto burstUsage = CpuBurstUsage.load() + CpuBurstUsageResume.load() + ReadBurstUsage.load();
    const auto burstThrottle = CpuBurstThrottle.load();
    newQuery->CpuBurstUsage = burstUsage - PrevCpuBurstUsage;
    newQuery->CpuBurstThrottle = burstThrottle - PrevCpuBurstThrottle;
    PrevCpuBurstUsage = burstUsage;
    PrevCpuBurstThrottle = burstThrottle;

    newQuery->CpuUsage = CpuUsage.load();
    newQuery->CpuThrottle = CpuThrottle.load();

    newQuery->MemoryDemand = MemoryDemand.load();

    return newQuery;
}

void TQuery::InitSnapshot(const NSnapshot::TPool& pool) {
    auto snapshot = std::make_shared<NSnapshot::TQuery>(std::get<TQueryId>(GetId()), shared_from_this());

    snapshot->CpuMaxDemand = TakeCpuMaxDemand();
    snapshot->CpuFairShare = pool.CpuFairShare > 0 ? pool.CpuFairShare : GetAllowMinFairShare();
    snapshot->MemoryFairShare = pool.MemoryFairShare;

    SetSnapshot(snapshot);
}

TFullPoolId TQuery::GetFullPoolId() const {
    auto* pool = GetParent();
    Y_ENSURE(pool, "Query " << std::get<TQueryId>(GetId()) << " is not attached to a pool");

    auto* database = pool;
    while (auto* parent = database->GetParent()) {
        if (parent->IsRoot()) {
            break;
        }
        database = parent;
    }

    Y_ENSURE(database->GetParent(), "Pool " << std::get<TPoolId>(pool->GetId()) << " is not attached to a database");
    Y_ENSURE(database != pool, "Query "
        << std::get<TQueryId>(GetId()) << " is attached to database "
        << std::get<TDatabaseId>(database->GetId()) << " instead of a pool");

    return {
        .DatabaseId = std::get<TDatabaseId>(database->GetId()),
        .PoolId = std::get<TPoolId>(pool->GetId()),
    };
}

bool TQuery::GetAllowMinFairShare() const {
    return GetCpuLimit() > 0;
}

ui64 TQuery::GetCpuLimit() const {
    ui64 limit = Infinity();
    if (const auto* parent = GetParent()) {
        ResolveStaticPool(*parent, &limit);
    }
    return limit;
}

ui64 TQuery::TakeCpuMaxDemand() {
    const ui64 demand = CpuMaxDemand.load();
    if (std::exchange(InitializeCpuDemand, false) && demand == 0) {
        const ui64 limit = GetCpuLimit();
        // An entirely unbounded policy has no finite headroom estimate; retain a one-slot start.
        return limit == Infinity() ? 1 : limit;
    }
    return demand;
}

TSchedulableTaskList::iterator TQuery::AddTask(const TSchedulableTaskPtr& task) {
    TGuard lock(TasksMutex);
    return SchedulableTasks.emplace(SchedulableTasks.end(), task, false);
}

ui32 TQuery::ResumeTasks(ui32 count) {
    TTryGuard lock(TasksMutex);
    ui32 run = 0;

    if (!lock) {
        // Either tasks are resumed somewhere else - which is ok, or we're adding new task - which is infrequent event.
        return run;
    }

    count = std::min<ui32>(count, SchedulableTasks.size());
    count = std::min<ui32>(count, CpuThrottle);

    for (auto it = SchedulableTasks.begin(); run < count && it != SchedulableTasks.end();) {
        if (auto task = it->first.lock()) {
            if (it->second) {
                task->Resume();
                ++run;
            }
            ++it;
        } else {
            // Lazy removal is acceptable since the query initially has constant number of tasks.
            it = SchedulableTasks.erase(it);
        }
    }

    return run;
}


///////////////////////////////////////////////////////////////////////////////
// TPool
///////////////////////////////////////////////////////////////////////////////

TPool::TPool(const TPoolId& id, const TIntrusivePtr<TKqpCounters>& counters)
    : NHdrf::TTreeElementBase<ETreeType::DYNAMIC>(id)
    , TTreeElement(id)
    , NHdrf::TPool<ETreeType::DYNAMIC>(id)
{
    if (!counters) {
        return;
    }

    auto group = counters->GetKqpCounters()->GetSubgroup("schedulerPool", id);

    // TODO: since counters don't support float-point values, then use CPU * 1'000'000 to account with microsecond precision

    Counters = TPoolCounters();
    Counters->Limit        = group->GetCounter("Limit",        false);
    Counters->Guarantee    = group->GetCounter("Guarantee",    false);
    Counters->EffectiveGuarantee = group->GetCounter("EffectiveGuarantee", true); // snapshot
    Counters->Demand       = group->GetCounter("Demand",       false); // snapshot
    Counters->ActualDemand = group->GetCounter("ActualDemand", true);  // snapshot
    Counters->InFlight     = group->GetCounter("InFlight",     false);
    Counters->Waiting      = group->GetCounter("Waiting",      false);
    Counters->Queries      = group->GetCounter("Queries",      false);
    Counters->Usage        = group->GetCounter("Usage",        true);
    Counters->UsageResume  = group->GetCounter("UsageResume",  true);
    Counters->Read         = group->GetCounter("Read",         true);
    Counters->Throttle     = group->GetCounter("Throttle",     true);
    Counters->FairShare    = group->GetCounter("FairShare",    true);  // snapshot

    Counters->Delay = group->GetHistogram("Delay",
        NMonitoring::ExplicitHistogram({10, 10e2, 10e3, 10e4, 10e5, 10e6, 10e7}), true); // TODO: make from MinDelay to MaxDelay.

    Counters->AdjustedSatisfaction = group->GetCounter("AdjustedSatisfaction", true); // snapshot

    Counters->MemoryLimit     = group->GetCounter("MemoryLimit",     false);
    Counters->MemoryUsage     = group->GetCounter("MemoryUsage",     false);
    Counters->MemoryDemand    = group->GetCounter("MemoryDemand",    false); // snapshot
    Counters->MemoryFairShare = group->GetCounter("MemoryFairShare", false); // snapshot
}

NStatic::TPoolPtr TPool::GetStaticPool() const {
    return ResolveStaticPool(*this);
}

TStaticAttributes TPool::GetConfiguredAttributes() const {
    const auto pool = GetStaticPool();
    return pool ? static_cast<const TStaticAttributes&>(*pool) : TStaticAttributes{};
}

NSnapshot::TPool* TPool::TakeSnapshot() {
    const auto staticPool = GetStaticPool();
    auto* newPool = CreateSnapshot(staticPool);
    if (staticPool) {
        const auto* root = staticPool.get();
        while (const auto* parent = root->GetParent()) {
            root = parent;
        }
        if (!static_cast<const NStatic::TRoot*>(root)->SupportsGuarantees()) {
            newPool->CpuGuarantee = 0;
        }
    }

    newPool->MemoryUsage = MemoryUsage.load();

    if (Counters) {
        const auto& attrs = newPool->GetConfiguredAttributes();
        Counters->Limit->Set(attrs.GetCpuLimit() * 1'000'000);
        Counters->Guarantee->Set(attrs.GetCpuGuarantee() * 1'000'000);
        Counters->InFlight->Set(CpuUsage * 1'000'000);
        Counters->Waiting->Set(CpuThrottle * 1'000'000);
        Counters->Usage->Set(CpuBurstUsage);
        Counters->UsageResume->Set(CpuBurstUsageResume);
        Counters->Read->Set(ReadBurstUsage);
        Counters->Throttle->Set(CpuBurstThrottle);
        Counters->MemoryLimit->Set(attrs.GetMemoryLimit());
        Counters->MemoryUsage->Set(MemoryUsage.load());
    }

    if (IsLeaf()) {
        if (Counters) {
            Counters->Queries->Set(ChildrenSize());
        }
        ForEachChild<TQuery>([&](TQuery* query, size_t) {
            newPool->AddQuery(NSnapshot::TQueryPtr(query->TakeSnapshot()));
        });
    } else {
        ForEachChild<TPool>([&](TPool* pool, size_t) {
            newPool->AddPool(NSnapshot::TPoolPtr(pool->TakeSnapshot()));
        });
    }

    return newPool;
}

NSnapshot::TPool* TPool::CreateSnapshot(const NStatic::TPoolPtr& pool) const {
    return new NSnapshot::TPool(std::get<TPoolId>(GetId()), Counters, pool);
}

///////////////////////////////////////////////////////////////////////////////
// TDefaultPool
///////////////////////////////////////////////////////////////////////////////

TDefaultPool::TDefaultPool(const TPoolId& id, const TIntrusivePtr<TKqpCounters>& counters)
    : NHdrf::TTreeElementBase<ETreeType::DYNAMIC>(id)
    , TPool(id, counters)
{
}

NSnapshot::TPool* TDefaultPool::CreateSnapshot(const NStatic::TPoolPtr& pool) const {
    return new NSnapshot::TDefaultPool(std::get<TPoolId>(GetId()), Counters, pool);
}

///////////////////////////////////////////////////////////////////////////////
// TDatabase
///////////////////////////////////////////////////////////////////////////////

TDatabase::TDatabase(const TDatabaseId& id)
    : NHdrf::TTreeElementBase<ETreeType::DYNAMIC>(id)
    , TPool(id, {})
{
}

NSnapshot::TDatabase* TDatabase::TakeSnapshot() {
    const auto staticPool = GetStaticPool();
    auto* newDatabase = new NSnapshot::TDatabase(std::get<TDatabaseId>(GetId()), staticPool);
    ForEachChild<TPool>([&](TPool* pool, size_t) {
        newDatabase->AddPool(NSnapshot::TPoolPtr(pool->TakeSnapshot()));
    });
    return newDatabase;
}

///////////////////////////////////////////////////////////////////////////////
// TRoot
///////////////////////////////////////////////////////////////////////////////

TRoot::TRoot(const TIntrusivePtr<TKqpCounters>& counters)
    : NHdrf::TTreeElementBase<ETreeType::DYNAMIC>("(ROOT)")
    , TPool("(ROOT)", {})
{
    Y_ASSERT(counters);
    auto group = counters->GetKqpCounters();
    Counters.TotalCpuLimit = group->GetCounter("scheduler/TotalLimit", false);
    Counters.TotalMemoryLimit = group->GetCounter("scheduler/TotalMemoryLimit", false);
}

void TRoot::AddDatabase(const TDatabasePtr& database) {
    AddPool(database);
}

void TRoot::RemoveDatabase(const TDatabaseId& databaseId) {
    RemovePool(databaseId);
}

TDatabasePtr TRoot::GetDatabase(const TDatabaseId& databaseId) const {
    return std::static_pointer_cast<TDatabase>(GetPool(databaseId));
}

NSnapshot::TRoot* TRoot::TakeSnapshot() {
    // The scheduler holds its tree lock until all children have captured the same static version.
    const auto tree = GetStaticTree();
    auto* newRoot = new NSnapshot::TRoot(tree);
    const ui64 totalCpuLimit = tree->TotalCpuLimit;
    const ui64 totalMemoryLimit = tree->TotalMemoryLimit;

    Counters.TotalCpuLimit->Set(totalCpuLimit * 1'000'000);
    Counters.TotalMemoryLimit->Set(totalMemoryLimit);

    ForEachChild<TDatabase>([&](TDatabase* database, size_t) {
        newRoot->AddDatabase(NSnapshot::TDatabasePtr(database->TakeSnapshot()));
    });
    return newRoot;
}

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic

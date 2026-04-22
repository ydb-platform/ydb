#include "dynamic.h"

#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_task.h>

namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic {

///////////////////////////////////////////////////////////////////////////////
// TTreeElement
///////////////////////////////////////////////////////////////////////////////

TPool* TTreeElement::GetParent() const {
    return dynamic_cast<TPool*>(Parent);
}

///////////////////////////////////////////////////////////////////////////////
// TQuery
///////////////////////////////////////////////////////////////////////////////

TQuery::TQuery(const TQueryId& id, const TDelayParams* delayParams, bool allowMinFairShare, const TStaticAttributes& attrs)
    : NHdrf::TTreeElementBase<ETreeType::DYNAMIC>(id, attrs)
    , TTreeElement(id, attrs)
    , NHdrf::TQuery<ETreeType::DYNAMIC>(id, attrs)
    , DelayParams(delayParams)
    , AllowMinFairShare(allowMinFairShare)
{
}

NSnapshot::TQuery* TQuery::TakeSnapshot() {
    auto* newQuery = new NSnapshot::TQuery(std::get<TQueryId>(GetId()), shared_from_this());

    // Take the average of original demand and actual demand, but keep at least 1 if original demand not zero.
    const auto demand = CpuDemand.load();
    newQuery->CpuDemand = (demand + CpuActualDemand.load()) >> 1;
    if (newQuery->CpuDemand == 0 && demand > 0) {
        newQuery->CpuDemand = 1;
    }
    CpuActualDemand = 0;

    // Update previous burst values and pass difference to new snapshot - to calculate adjusted satisfaction
    const auto burstUsage = CpuBurstUsage.load() + CpuBurstUsageResume.load();
    const auto burstThrottle = CpuBurstThrottle.load();
    newQuery->CpuBurstUsage += burstUsage - PrevCpuBurstUsage;
    newQuery->CpuBurstThrottle = burstThrottle - PrevCpuBurstThrottle;
    PrevCpuBurstUsage = burstUsage;
    PrevCpuBurstThrottle = burstThrottle;

    newQuery->CpuUsage = CpuUsage.load();
    return newQuery;
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

void TQuery::UpdateActualDemand() {
    auto demand = CpuUsage + CpuThrottle + 1;
    auto actualDemand = CpuActualDemand.load();
    while (actualDemand < demand && !CpuActualDemand.compare_exchange_weak(actualDemand, demand)) {}
}

///////////////////////////////////////////////////////////////////////////////
// TPool
///////////////////////////////////////////////////////////////////////////////

TPool::TPool(const TPoolId& id, const TIntrusivePtr<TKqpCounters>& counters, const TStaticAttributes& attrs)
    : NHdrf::TTreeElementBase<ETreeType::DYNAMIC>(id, attrs)
    , TTreeElement(id, attrs)
    , NHdrf::TPool<ETreeType::DYNAMIC>(id, attrs)
{
    if (!counters) {
        return;
    }

    auto group = counters->GetKqpCounters()->GetSubgroup("schedulerPool", id);

    // TODO: since counters don't support float-point values, then use CPU * 1'000'000 to account with microsecond precision

    Counters = TPoolCounters();
    Counters->Satisfaction = group->GetCounter("Satisfaction", false); // snapshot
    Counters->Limit        = group->GetCounter("Limit",        false);
    Counters->Guarantee    = group->GetCounter("Guarantee",    false);
    Counters->Demand       = group->GetCounter("Demand",       false); // snapshot
    Counters->InFlight     = group->GetCounter("InFlight",     false);
    Counters->Waiting      = group->GetCounter("Waiting",      false);
    Counters->Queries      = group->GetCounter("Queries",      false);
    Counters->Usage        = group->GetCounter("Usage",        true);
    Counters->UsageResume  = group->GetCounter("UsageResume",  true);
    Counters->Throttle     = group->GetCounter("Throttle",     true);
    Counters->FairShare    = group->GetCounter("FairShare",    true);  // snapshot

    Counters->Delay = group->GetHistogram("Delay",
        NMonitoring::ExplicitHistogram({10, 10e2, 10e3, 10e4, 10e5, 10e6, 10e7}), true); // TODO: make from MinDelay to MaxDelay.

    Counters->AdjustedSatisfaction = group->GetCounter("AdjustedSatisfaction", true); // snapshot
}

NSnapshot::TPool* TPool::TakeSnapshot() {
    auto* newPool = new NSnapshot::TPool(std::get<TPoolId>(GetId()), Counters, *this);

    if (Counters) {
        Counters->Limit->Set(GetCpuLimit() * 1'000'000);
        Counters->Guarantee->Set(GetCpuGuarantee() * 1'000'000);
        Counters->InFlight->Set(CpuUsage * 1'000'000);
        Counters->Waiting->Set(CpuThrottle * 1'000'000);
        Counters->Usage->Set(CpuBurstUsage);
        Counters->UsageResume->Set(CpuBurstUsageResume);
        Counters->Throttle->Set(CpuBurstThrottle);
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

///////////////////////////////////////////////////////////////////////////////
// TDatabase
///////////////////////////////////////////////////////////////////////////////

TDatabase::TDatabase(const TDatabaseId& id, const TStaticAttributes& attrs)
    : NHdrf::TTreeElementBase<ETreeType::DYNAMIC>(id, attrs)
    , TPool(id, {}, attrs)
{
}

NSnapshot::TDatabase* TDatabase::TakeSnapshot() {
    auto* newDatabase = new NSnapshot::TDatabase(std::get<TDatabaseId>(GetId()), *this);
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
    Counters.TotalLimit = group->GetCounter("scheduler/TotalLimit", false);
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
    auto* newRoot = new NSnapshot::TRoot();

    Counters.TotalLimit->Set(TotalLimit * 1'000'000);

    newRoot->TotalLimit = TotalLimit;
    ForEachChild<TDatabase>([&](TDatabase* database, size_t) {
        newRoot->AddDatabase(NSnapshot::TDatabasePtr(database->TakeSnapshot()));
    });

    return newRoot;
}

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic

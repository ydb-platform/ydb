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

TQuery::TQuery(const TQueryId& id, const TDelayParams* delayParams, const TStaticAttributes& attrs)
    : NHdrf::TTreeElementBase<ETreeType::DYNAMIC>(id, attrs)
    , TTreeElement(id, attrs)
    , NHdrf::TQuery<ETreeType::DYNAMIC>(id, attrs)
    , DelayParams(delayParams)
{
}

NSnapshot::TQuery* TQuery::TakeSnapshot() const {
    auto* newQuery = new NSnapshot::TQuery(std::get<TQueryId>(GetId()), const_cast<TQuery*>(this));
    newQuery->Demand = Demand.load();
    newQuery->Usage = Usage.load();
    return newQuery;
}

TSchedulableTaskList::iterator TQuery::AddTask(const TSchedulableTaskPtr& task) {
    TWriteGuard lock(TasksMutex);
    return SchedulableTasks.emplace(SchedulableTasks.end(), task, false);
}

void TQuery::RemoveTask(const TSchedulableTaskList::iterator& it) {
    TWriteGuard lock(TasksMutex);
    SchedulableTasks.erase(it);
}

ui32 TQuery::ResumeTasks(ui32 count) {
    TReadGuard lock(TasksMutex);
    ui32 run = 0;

    count = std::min<ui32>(count, SchedulableTasks.size());
    count = std::min<ui32>(count, Throttle);

    for (auto it = SchedulableTasks.begin(); run < count && it != SchedulableTasks.end(); ++it) {
        if (it->second) {
            if (auto task = it->first.lock()) {
                task->Resume();
                ++run;
            }
        }
    }

    return run;
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

    auto group = counters->GetKqpCounters()->GetSubgroup("scheduler/pool", id);

    // TODO: since counters don't support float-point values, then use CPU * 1'000'000 to account with microsecond precision

    Counters = TPoolCounters();
    Counters->Limit     = group->GetCounter("Limit",     false);
    Counters->Guarantee = group->GetCounter("Guarantee", false);
    Counters->Demand    = group->GetCounter("Demand",    false); // snapshot
    Counters->InFlight  = group->GetCounter("InFlight",  false);
    Counters->Waiting   = group->GetCounter("Waiting",   false);
    Counters->Usage     = group->GetCounter("Usage",     true);
    Counters->Throttle  = group->GetCounter("Throttle",  true);
    Counters->FairShare = group->GetCounter("FairShare", true);  // snapshot

    Counters->InFlightExtra = group->GetCounter("InFlightExtra",  false);
    Counters->UsageExtra    = group->GetCounter("UsageExtra",     true);

    Counters->Delay     = group->GetHistogram("Delay",
        NMonitoring::ExplicitHistogram({10, 10e2, 10e3, 10e4, 10e5, 10e6, 10e7}), true); // TODO: make from MinDelay to MaxDelay.
}

NSnapshot::TPool* TPool::TakeSnapshot() const {
    auto* newPool = new NSnapshot::TPool(std::get<TPoolId>(GetId()), Counters, *this);

    if (Counters) {
        Counters->Limit->Set(GetLimit() * 1'000'000);
        Counters->Guarantee->Set(GetGuarantee() * 1'000'000);
        Counters->InFlight->Set(Usage * 1'000'000);
        Counters->Waiting->Set(Throttle * 1'000'000);
        Counters->Usage->Set(BurstUsage);
        Counters->Throttle->Set(BurstThrottle);

        Counters->InFlightExtra->Set(UsageExtra * 1'000'000);
        Counters->UsageExtra->Set(BurstUsageExtra);
    }

    if (IsLeaf()) {
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

NSnapshot::TDatabase* TDatabase::TakeSnapshot() const {
    auto* newDatabase = new NSnapshot::TDatabase(std::get<TDatabaseId>(GetId()), *this);
    ForEachChild<TPool>([&](TPool* pool, size_t) {
        newDatabase->AddPool(NSnapshot::TPoolPtr(pool->TakeSnapshot()));
    });
    return newDatabase;
}

///////////////////////////////////////////////////////////////////////////////
// TRoot
///////////////////////////////////////////////////////////////////////////////

TRoot::TRoot(TIntrusivePtr<TKqpCounters> counters)
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

NSnapshot::TRoot* TRoot::TakeSnapshot() const {
    auto* newRoot = new NSnapshot::TRoot();

    Counters.TotalLimit->Set(TotalLimit * 1'000'000);

    newRoot->TotalLimit = TotalLimit;
    ForEachChild<TDatabase>([&](TDatabase* database, size_t) {
        newRoot->AddDatabase(NSnapshot::TDatabasePtr(database->TakeSnapshot()));
    });

    return newRoot;
}

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic

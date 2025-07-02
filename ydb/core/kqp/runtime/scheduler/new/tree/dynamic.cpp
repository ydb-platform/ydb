#include "dynamic.h"

namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic {

///////////////////////////////////////////////////////////////////////////////
// TTreeElementBase
///////////////////////////////////////////////////////////////////////////////

void TTreeElementBase::AddChild(const TTreeElementPtr& element) {
    const auto usage = element->Usage.load();
    const auto throttle = element->Throttle.load();
    const auto burstUsage = element->BurstUsage.load();
    const auto burstThrottle = element->BurstThrottle.load();

    Children.push_back(element);
    element->Parent = this;

    // In case of detached query we should propagate approximate values to new parents.
    for (TTreeElementBase* parent = this; parent; parent = parent->Parent) {
        parent->Usage += usage;
        parent->Throttle += throttle;
        parent->BurstUsage += burstUsage;
        parent->BurstThrottle += burstThrottle;
    }
}

void TTreeElementBase::RemoveChild(const TTreeElementPtr& element) {
    for (auto it = Children.begin(); it != Children.end(); ++it) {
        if (*it == element) {
            element->Parent = nullptr;
            // TODO: should we decrease usage, throttle, etc for parents here?
            Children.erase(it);
            return;
        }
    }
    // TODO: throw exception that child not found.
}

///////////////////////////////////////////////////////////////////////////////
// TQuery
///////////////////////////////////////////////////////////////////////////////

TQuery::TQuery(const TQueryId& id, const TStaticAttributes& attrs)
    : Id(id)
{
    Update(attrs);
}

NSnapshot::TQuery* TQuery::TakeSnapshot() const {
    auto* newQuery = new NSnapshot::TQuery(GetId(), const_cast<TQuery*>(this));
    newQuery->Demand = Demand.load();
    return newQuery;
}

///////////////////////////////////////////////////////////////////////////////
// TPool
///////////////////////////////////////////////////////////////////////////////

TPool::TPool(const TString& id, const TIntrusivePtr<TKqpCounters>& counters, const TStaticAttributes& attrs)
    : Id(id)
{
    Update(attrs);

    Y_ENSURE(counters);

    auto group = counters->GetKqpCounters()->GetSubgroup("scheduler/pool", Id);

    // TODO: since counters don't support float-point values, then use CPU * 1'000'000 to account with microsecond precision

    Counters.Limit     = group->GetCounter("Limit",     false);
    Counters.Guarantee = group->GetCounter("Guarantee", false);
    Counters.Demand    = group->GetCounter("Demand",    false); // snapshot
    Counters.InFlight  = group->GetCounter("InFlight",  false);
    Counters.Waiting   = group->GetCounter("Waiting",   false);
    Counters.Usage     = group->GetCounter("Usage",     true);
    Counters.Throttle  = group->GetCounter("Throttle",  true);
    Counters.FairShare = group->GetCounter("FairShare", true);  // snapshot

    Counters.InFlightExtra = group->GetCounter("InFlightExtra",  false);
    Counters.UsageExtra    = group->GetCounter("UsageExtra",     true);

    Counters.Delay     = group->GetHistogram("Delay",
        NMonitoring::ExplicitHistogram({10, 10e2, 10e3, 10e4, 10e5, 10e6, 10e7}), true); // TODO: make from MinDelay to MaxDelay.
}

NSnapshot::TPool* TPool::TakeSnapshot() const {
    auto* newPool = new NSnapshot::TPool(GetId(), Counters, *this);

    Counters.Limit->Set(GetLimit() * 1'000'000);
    Counters.Guarantee->Set(GetGuarantee() * 1'000'000);
    Counters.InFlight->Set(Usage * 1'000'000);
    Counters.Waiting->Set(Throttle * 1'000'000);
    Counters.Usage->Set(BurstUsage);
    Counters.Throttle->Set(BurstThrottle);

    Counters.InFlightExtra->Set(UsageExtra * 1'000'000);
    Counters.UsageExtra->Set(BurstUsageExtra);

    for (const auto& child : Children) {
        newPool->AddQuery(std::shared_ptr<NSnapshot::TQuery>(std::dynamic_pointer_cast<TQuery>(child)->TakeSnapshot()));
    }

    return newPool;
}

void TPool::AddQuery(const TQueryPtr& query) {
    Y_ENSURE(!Queries.contains(query->GetId()));
    Queries.emplace(query->GetId(), query);
    AddChild(query);

    query->Delay = Counters.Delay;
}

void TPool::RemoveQuery(const TQueryId& queryId) {
    auto queryIt = Queries.find(queryId);
    Y_ENSURE(queryIt != Queries.end());
    RemoveChild(queryIt->second);
    Queries.erase(queryIt);
}

TQueryPtr TPool::GetQuery(const TQueryId& queryId) const {
    auto it = Queries.find(queryId);
    return it == Queries.end() ? nullptr : it->second;
}

///////////////////////////////////////////////////////////////////////////////
// TDatabase
///////////////////////////////////////////////////////////////////////////////

TDatabase::TDatabase(const TString& id, const TStaticAttributes& attrs)
    : Id(id)
{
    Update(attrs);
}

NSnapshot::TDatabase* TDatabase::TakeSnapshot() const {
    auto* newDatabase = new NSnapshot::TDatabase(GetId(), *this);
    for (const auto& child : Children) {
        newDatabase->AddPool(std::shared_ptr<NSnapshot::TPool>(std::dynamic_pointer_cast<TPool>(child)->TakeSnapshot()));
    }
    return newDatabase;
}

void TDatabase::AddPool(const TPoolPtr& pool) {
    Y_ENSURE(!Pools.contains(pool->GetId()));
    Pools.emplace(pool->GetId(), pool);
    AddChild(pool);
}

TPoolPtr TDatabase::GetPool(const TString& poolId) const {
    auto it = Pools.find(poolId);
    return it == Pools.end() ? nullptr : it->second;
}

///////////////////////////////////////////////////////////////////////////////
// TRoot
///////////////////////////////////////////////////////////////////////////////

TRoot::TRoot(TIntrusivePtr<TKqpCounters> counters) {
    auto group = counters->GetKqpCounters();
    Counters.TotalLimit = group->GetCounter("scheduler/TotalLimit", false);
}

void TRoot::AddDatabase(const TDatabasePtr& database) {
    Y_ENSURE(!Databases.contains(database->GetId()));
    Databases.emplace(database->GetId(), database);
    AddChild(database);
}

TDatabasePtr TRoot::GetDatabase(const TString& id) const {
    auto it = Databases.find(id);
    return it == Databases.end() ? nullptr : it->second;
}

NSnapshot::TRoot* TRoot::TakeSnapshot() const {
    auto* newRoot = new NSnapshot::TRoot();

    Counters.TotalLimit->Set(TotalLimit * 1'000'000);

    newRoot->TotalLimit = TotalLimit;
    for (const auto& child : Children) {
        newRoot->AddDatabase(std::shared_ptr<NSnapshot::TDatabase>(std::dynamic_pointer_cast<TDatabase>(child)->TakeSnapshot()));
    }

    return newRoot;
}

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic

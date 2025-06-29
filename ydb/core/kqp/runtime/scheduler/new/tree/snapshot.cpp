#include "snapshot.h"

#include "dynamic.h"

namespace NKikimr::NKqp::NScheduler::NHdrf::NSnapshot {

void TTreeElementBase::AddChild(const TTreeElementPtr& element) {
    Children.push_back(element);
    element->Parent = this;
}

void TTreeElementBase::RemoveChild(const TTreeElementPtr& element) {
    for (auto it = Children.begin(); it != Children.end(); ++it) {
        if (*it == element) {
            element->Parent = nullptr;
            // TODO: should we decrease usage for parents here?
            Children.erase(it);
            return;
        }
    }
    // TODO: throw exception that child not found.
}

void TTreeElementBase::AccountFairShare(const TDuration& period) {
    for (auto& child : Children) {
        child->AccountFairShare(period);
    }
}

void TTreeElementBase::UpdateBottomUp(ui64 totalLimit) {
    TotalLimit = totalLimit;
    Limit = Min<ui64>(GetLimit(), TotalLimit);

    if (!IsLeaf()) {
        Demand = 0;
        for (auto& child : Children) {
            child->UpdateBottomUp(totalLimit);
            Demand += child->Demand;
        }
    }

    Demand = Min<ui64>(Demand, GetLimit());
    Guarantee = Min<ui64>(GetGuarantee(), Demand);
}

void TTreeElementBase::UpdateTopDown() {
    if (IsRoot()) {
        FairShare = Demand;
    }

    // At this moment we know own fair-share. Need to calibrate children.

    // Fair-share variant (for pools and databases)
    if (!IsLeaf() && !Children.at(0)->IsLeaf()) {
        ui64 totalWeightedDemand = 0;
        std::vector<ui64> weightedDemand;

        weightedDemand.resize(Children.size());
        for (size_t i = 0, s = Children.size(); i < s; ++i) {
            const auto& child = Children.at(i);
            weightedDemand.at(i) = child->GetWeight() * child->Demand;
            totalWeightedDemand += weightedDemand.at(i);
        }

        for (size_t i = 0, s = Children.size(); i < s; ++i) {
            const auto& child = Children.at(i);
            if (totalWeightedDemand > 0) {
                // TODO: distribute resources lost because of integer division.
                child->FairShare = weightedDemand.at(i) * FairShare / totalWeightedDemand;
            } else {
                child->FairShare = 0;
            }
            child->UpdateTopDown();
        }
    }
    // FIFO variant (for queries)
    else {
        // TODO: stable sort children by weight

        auto leftFairShare = FairShare;

        // Give at least 1 fair-share for each demanding child
        for (size_t i = 0, s = Children.size(); i < s && leftFairShare > 0; ++i) {
            const auto& child = Children.at(i);
            if (child->Demand > 0) {
                child->FairShare = 1;
                --leftFairShare;
            }
        }

        for (size_t i = 0, s = Children.size(); i < s; ++i) {
            const auto& child = Children.at(i);
            auto demand = child->Demand > 0 ? child->Demand - 1 : 0;
            child->FairShare += Min(leftFairShare, demand);
            leftFairShare = leftFairShare <= demand ? 0 : leftFairShare - demand;

            // TODO: looks a little bit hacky.
            if (auto query = std::dynamic_pointer_cast<TQuery>(child); query && query->Origin) {
                query->Origin->SetSnapshot(query);
                query->Origin = nullptr;
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// TQuery
///////////////////////////////////////////////////////////////////////////////

TQuery::TQuery(const TQueryId& id, NDynamic::TQuery* origQuery)
    : Origin(origQuery)
    , Id(id)
{
    Update(*origQuery);
}

///////////////////////////////////////////////////////////////////////////////
// TPool
///////////////////////////////////////////////////////////////////////////////

TPool::TPool(const TString& id, const TPoolCounters& counters, const TStaticAttributes& attrs)
    : Id(id)
{
    Update(attrs);

    Counters.Demand    = counters.Demand;
    Counters.FairShare = counters.FairShare;
}

void TPool::AccountFairShare(const TDuration& period) {
    Counters.FairShare->Add(FairShare * period.MicroSeconds());
    TTreeElementBase::AccountFairShare(period);
}

void TPool::UpdateBottomUp(ui64 totalLimit) {
    TTreeElementBase::UpdateBottomUp(totalLimit);
    Counters.Demand->Set(Demand * 1'000'000);
}

void TPool::AddQuery(const TQueryPtr& query) {
    Y_ENSURE(!Queries.contains(query->GetId()));
    Queries.emplace(query->GetId(), query);
    AddChild(query);
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

void TRoot::AddDatabase(const TDatabasePtr& database) {
    Y_ENSURE(!Databases.contains(database->GetId()));
    Databases.emplace(database->GetId(), database);
    AddChild(database);
}

void TRoot::AccountFairShare(const TRootPtr& previous) {
    AccountFairShare(Timestamp - previous->Timestamp);
}

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NSnapshot

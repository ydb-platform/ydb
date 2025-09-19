#include "snapshot.h"

#include "dynamic.h"

namespace NKikimr::NKqp::NScheduler::NHdrf::NSnapshot {

///////////////////////////////////////////////////////////////////////////////
// TTreeElement
///////////////////////////////////////////////////////////////////////////////

TPool* TTreeElement::GetParent() const {
    return dynamic_cast<TPool*>(Parent);
}

void TTreeElement::AccountSnapshotDuration(const TDuration& period) {
    ForEachChild<TTreeElement>([&](TTreeElement* child, size_t) {
        child->AccountSnapshotDuration(period);
    });
}

void TTreeElement::UpdateBottomUp(ui64 totalLimit) {
    TotalLimit = totalLimit;
    Limit = Min<ui64>(GetLimit(), TotalLimit);

    if (IsPool()) {
        Demand = 0;
        Usage = 0;
        ForEachChild<TTreeElement>([&](TTreeElement* child, size_t) {
            child->UpdateBottomUp(totalLimit);
            Demand += child->Demand;
            Usage += child->Usage;
        });
    }

    Demand = Min<ui64>(Demand, GetLimit());
    Guarantee = Min<ui64>(GetGuarantee(), Demand);
}

void TTreeElement::UpdateTopDown() {
    if (IsRoot()) {
        FairShare = Demand;
    }

    // At this moment we know own fair-share. Need to calibrate children.

    if (FairShare) {
        Satisfaction = Usage / float(FairShare);
    }

    if (!IsPool()) {
        return;
    }

    // Fair-share variant (when children are pools and databases)
    if (!IsLeaf()) {
        ui64 totalWeightedDemand = 0;
        std::vector<ui64> weightedDemand;

        weightedDemand.resize(ChildrenSize());
        ForEachChild<TTreeElement>([&](TTreeElement* child, size_t i) {
            weightedDemand.at(i) = child->GetWeight() * child->Demand;
            totalWeightedDemand += weightedDemand.at(i);
        });
        ForEachChild<TTreeElement>([&](TTreeElement* child, size_t i) {
            if (totalWeightedDemand > 0) {
                // TODO: distribute the resources lost cause of integer division.
                child->FairShare = weightedDemand.at(i) * FairShare / totalWeightedDemand;
            } else {
                child->FairShare = 0;
            }

            child->UpdateTopDown();
        });
    }
    // FIFO variant (when children are queries)
    else {
        // TODO: stable sort children by weight

        auto leftFairShare = FairShare;

        // Give at least 1 fair-share for each demanding child
        ForEachChild<TTreeElement>([&](TTreeElement* child, size_t) -> bool {
            if (leftFairShare == 0) {
                return true;
            }

            if (child->Demand > 0) {
                child->FairShare = 1;
                --leftFairShare;
            }

            return false;
        });
        ForEachChild<TQuery>([&](TQuery* query, size_t) {
            auto demand = query->Demand > 0 ? query->Demand - 1 : 0;
            query->FairShare += Min(leftFairShare, demand);
            leftFairShare = leftFairShare <= demand ? 0 : leftFairShare - demand;

            if (auto originalQuery = query->Origin.lock()) {
                originalQuery->SetSnapshot(query->shared_from_this());
            }
        });
    }
}

///////////////////////////////////////////////////////////////////////////////
// TQuery
///////////////////////////////////////////////////////////////////////////////

TQuery::TQuery(const TQueryId& id, NDynamic::TQueryPtr query)
    : NHdrf::TTreeElementBase<ETreeType::SNAPSHOT>(id, *query)
    , TTreeElement(id, *query)
    , NHdrf::TQuery<ETreeType::SNAPSHOT>(id, *query)
    , Origin(query)
{
}

///////////////////////////////////////////////////////////////////////////////
// TPool
///////////////////////////////////////////////////////////////////////////////

TPool::TPool(const TPoolId& id, const std::optional<TPoolCounters>& counters, const TStaticAttributes& attrs)
    : NHdrf::TTreeElementBase<ETreeType::SNAPSHOT>(id, attrs)
    , TTreeElement(id, attrs)
    , NHdrf::TPool<ETreeType::SNAPSHOT>(id, attrs)
{
    if (counters) {
        Counters = TPoolCounters();
        Counters->Satisfaction = counters->Satisfaction;
        Counters->Demand    = counters->Demand;
        Counters->FairShare = counters->FairShare;
    }
}

void TPool::AccountSnapshotDuration(const TDuration& period) {
    if (Counters) {
        Counters->FairShare->Add(FairShare * period.MicroSeconds());
        Counters->Satisfaction->Set(Satisfaction.value_or(0) * 1'000'000);
        // TODO: calculate satisfaction as burst-usage increment / fair-share increment - for better precision
    }
    TTreeElement::AccountSnapshotDuration(period);
}

void TPool::UpdateBottomUp(ui64 totalLimit) {
    TTreeElement::UpdateBottomUp(totalLimit);
    if (Counters) {
        Counters->Demand->Set(Demand * 1'000'000);
    }
}

///////////////////////////////////////////////////////////////////////////////
// TDatabase
///////////////////////////////////////////////////////////////////////////////

TDatabase::TDatabase(const TDatabaseId& id, const TStaticAttributes& attrs)
    : NHdrf::TTreeElementBase<ETreeType::SNAPSHOT>(id, attrs)
    , TPool(id, {}, attrs)
{
}

///////////////////////////////////////////////////////////////////////////////
// TRoot
///////////////////////////////////////////////////////////////////////////////

TRoot::TRoot()
    : NHdrf::TTreeElementBase<ETreeType::SNAPSHOT>("(ROOT)")
    , TPool("(ROOT)", {})
{
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

void TRoot::AccountPreviousSnapshot(const TRootPtr& snapshot) {
    AccountSnapshotDuration(Timestamp - snapshot->Timestamp);
}

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NSnapshot

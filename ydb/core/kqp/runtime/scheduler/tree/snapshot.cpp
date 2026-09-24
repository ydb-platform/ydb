#include "snapshot.h"

#include <algorithm>

#include "dynamic.h" // IWYU pragma: keep

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
    CpuLimit = Min<ui64>(GetCpuLimit(), TotalLimit);

    if (IsPool()) {
        CpuDemand = 0;
        CpuUsage = 0;
        CpuBurstUsage = 0;
        CpuBurstThrottle = 0;
        ReadBurstUsage = 0;
        ForEachChild<TTreeElement>([&](TTreeElement* child, size_t) {
            child->UpdateBottomUp(totalLimit);
            CpuDemand += child->CpuDemand;
            CpuUsage += child->CpuUsage;
            CpuBurstUsage += child->CpuBurstUsage;
            CpuBurstThrottle += child->CpuBurstThrottle;
            ReadBurstUsage += child->ReadBurstUsage;
        });
    }

    CpuDemand = Min<ui64>(CpuDemand, GetCpuLimit());
}

namespace {

// Progressive filling: the fair-share is split proportionally to the weights, but a child is never
// given more than its unsatisfied demand - a child whose whole demand fits into its proportion takes
// exactly the demand, and what it leaves behind is split among the rest on the next round.
// Every round satisfies at least one child, so there are at most as many rounds as children.
// Returns the fair-share which is left undistributed.
ui64 FillDemand(const std::vector<TTreeElement*>& children, std::vector<ui64>& unsatisfiedDemand, ui64 leftFairShare) {
    for (bool satisfied = true; satisfied && leftFairShare > 0; ) {
        satisfied = false;

        // Recalculated on every round on purpose: subtracting the weight of a satisfied child would
        // accumulate a floating point error and could leave a bogus non-zero total behind.
        double totalWeight = 0;
        for (size_t i = 0; i < children.size(); ++i) {
            if (unsatisfiedDemand.at(i) > 0) {
                totalWeight += children.at(i)->GetWeight();
            }
        }

        if (totalWeight <= 0) {
            break; // nobody is asking for more
        }

        for (size_t i = 0; i < children.size(); ++i) {
            // The proportion of the child is (weight * leftFairShare / totalWeight) - compared
            // against the demand without the division to keep the check exact.
            if (unsatisfiedDemand.at(i) > 0 &&
                children.at(i)->GetWeight() * leftFairShare >= unsatisfiedDemand.at(i) * totalWeight)
            {
                children.at(i)->FairShare += unsatisfiedDemand.at(i);
                leftFairShare -= unsatisfiedDemand.at(i);
                unsatisfiedDemand.at(i) = 0;
                satisfied = true;
            }
        }

        if (!satisfied) {
            // Everyone left demands more than its proportion, so the rest is split as it is. The proportions
            // are rounded down, and the CPUs lost on rounding are given one by one to the children with
            // the largest fractional parts (the largest remainder method) - otherwise a child with a small
            // weight could get no CPU at all, and up to a CPU per child would be left idling.
            // Every child here demands more than its proportion, so an extra CPU never exceeds its demand.
            std::vector<std::pair<double, size_t>> remainders;
            ui64 given = 0;
            for (size_t i = 0; i < children.size(); ++i) {
                if (unsatisfiedDemand.at(i) > 0) {
                    const double proportion = children.at(i)->GetWeight() * leftFairShare / totalWeight;
                    const auto share = Min<ui64>(static_cast<ui64>(proportion), leftFairShare - given);
                    children.at(i)->FairShare += share;
                    unsatisfiedDemand.at(i) -= share;
                    given += share;
                    remainders.emplace_back(proportion - share, i);
                }
            }
            leftFairShare -= given;

            // Stable, so the ties are resolved in the same order on every node.
            // TODO: the same children win the ties on every snapshot - with more children than CPUs the rest
            //       get nothing for as long as the contention lasts.
            std::ranges::stable_sort(remainders, [](const auto& left, const auto& right) {
                return left.first > right.first;
            });
            for (const auto& [_, i] : remainders) {
                if (leftFairShare == 0) {
                    break;
                }
                if (unsatisfiedDemand.at(i) > 0) {
                    ++children.at(i)->FairShare;
                    --unsatisfiedDemand.at(i);
                    --leftFairShare;
                }
            }
        }
    }

    return leftFairShare;
}

} // namespace

void TTreeElement::DistributeFairShare() {
    std::vector<TTreeElement*> children(ChildrenSize());
    std::vector<ui64> unsatisfiedDemand(ChildrenSize());

    ForEachChild<TTreeElement>([&](TTreeElement* child, size_t i) {
        children.at(i) = child;
        child->FairShare = 0;
        unsatisfiedDemand.at(i) = child->CpuDemand;
    });

    FillDemand(children, unsatisfiedDemand, FairShare);
}

void TTreeElement::UpdateTopDown() {
    if (IsRoot()) {
        FairShare = CpuDemand;
    }

    // At this moment we know own fair-share. Need to calibrate children.

    if (!IsPool()) {
        return;
    }

    // Fair-share variant (when children are pools and databases)
    if (!IsLeaf()) {
        DistributeFairShare();

        ForEachChild<TTreeElement>([&](TTreeElement* child, size_t) {
            child->UpdateTopDown();
        });
    }
    // All-equal variant (when children are queries)
    // TODO: it's workaround mode - the queries should get their own fair-share in the future.
    else {
        ForEachChild<TQuery>([&](TQuery* query, size_t) {
            if (query->CpuDemand > 0) {
                query->FairShare = FairShare;
            }

            if (auto originalQuery = query->Origin.lock()) {
                originalQuery->SetSnapshot(query->shared_from_this());
            }
        });
    }
}

///////////////////////////////////////////////////////////////////////////////
// TQuery
///////////////////////////////////////////////////////////////////////////////

TQuery::TQuery(const TQueryId& id, const NDynamic::TQueryPtr& query)
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
        Counters->AdjustedSatisfaction = counters->AdjustedSatisfaction;
        Counters->Demand    = counters->Demand;
        Counters->FairShare = counters->FairShare;
    }
}

void TPool::AccountSnapshotDuration(const TDuration& period) {
    if (Counters) {
        const auto fairShare = FairShare * period.MicroSeconds();

        Counters->FairShare->Add(fairShare);

        const auto wanted = CpuBurstUsage + CpuBurstThrottle;
        float adjustedSatisfaction = 1.0; // nothing was wanted - so nothing is missing
        if (wanted > 0) {
            if (auto adjustedFairShare = std::min(fairShare, wanted)) {
                adjustedSatisfaction = CpuBurstUsage / (float)adjustedFairShare;
            } else if (CpuBurstUsage == 0) {
                adjustedSatisfaction = 0.0; // wanted but got no fair-share at all - starving
            }
        }
        Counters->AdjustedSatisfaction->Add(adjustedSatisfaction * period.MicroSeconds());
    }
    TTreeElement::AccountSnapshotDuration(period);
}

void TPool::UpdateBottomUp(ui64 totalLimit) {
    TTreeElement::UpdateBottomUp(totalLimit);
    if (Counters) {
        Counters->Demand->Set(CpuDemand * 1'000'000);
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

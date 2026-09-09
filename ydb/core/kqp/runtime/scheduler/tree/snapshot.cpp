#include "snapshot.h"

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

    // The guarantee is configured for the leaves - the pools that hold the queries - so a leaf keeps
    // its own one, while an intermediate element reserves exactly what its children reserve.
    if (!IsLeaf()) {
        CpuGuarantee = GetChildrenCpuGuarantee();
    }

    CpuDemand = Min<ui64>(CpuDemand, GetCpuLimit());
    CpuGuarantee = Min<ui64>(GetCpuGuarantee(), CpuDemand);
}

void TTreeElement::DistributeFairShare() {
    const ui64 totalGuaranteedShare = GetChildrenCpuGuarantee();

    if (totalGuaranteedShare >= FairShare) {
        ForEachChild<TTreeElement>([&](TTreeElement* child, size_t) {
            // TODO: distribute the resources lost cause of integer division.
            child->FairShare = totalGuaranteedShare > 0
                ? child->GetCpuGuarantee() * FairShare / totalGuaranteedShare
                : 0;
        });
        return;
    }

    // Every child gets its guaranteed share first, and the rest is distributed by the weighted demand.
    std::vector<TTreeElement*> children(ChildrenSize());
    std::vector<ui64> unsatisfiedDemand(ChildrenSize());

    ForEachChild<TTreeElement>([&](TTreeElement* child, size_t i) {
        // The guarantee has already been capped by the demand in UpdateBottomUp().
        Y_ASSERT(child->CpuDemand >= child->GetCpuGuarantee());
        children.at(i) = child;
        child->FairShare = child->GetCpuGuarantee();
        unsatisfiedDemand.at(i) = child->CpuDemand - child->GetCpuGuarantee();
    });

    ui64 leftFairShare = FairShare - totalGuaranteedShare;

    // Progressive filling: the leftover is split proportionally to the weights, but a child is never
    // given more than it is able to use - a child whose whole demand fits into its proportion takes
    // exactly the demand, and what it leaves behind is split among the rest on the next round.
    // Every round satisfies at least one child, so there are at most as many rounds as children.
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
            return; // nobody is asking for more than its guarantee
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
            // Everyone left demands more than its proportion, so the rest is split as it is.
            for (size_t i = 0; i < children.size(); ++i) {
                if (unsatisfiedDemand.at(i) > 0) {
                    // TODO: distribute the resources lost cause of integer division.
                    children.at(i)->FairShare += children.at(i)->GetWeight() * leftFairShare / totalWeight;
                }
            }
        }
    }
}

void TTreeElement::UpdateTopDown(ELeafFairShare fairShareMode) {
    if (IsRoot()) {
        FairShare = CpuDemand;
    }

    // At this moment we know own fair-share. Need to calibrate children.

    if (FairShare) {
        Satisfaction = CpuUsage / float(FairShare);
    }

    if (!IsPool()) {
        return;
    }

    // Fair-share variant (when children are pools and databases)
    if (!IsLeaf()) {
        DistributeFairShare();

        ForEachChild<TTreeElement>([&](TTreeElement* child, size_t) {
            child->UpdateTopDown(fairShareMode);
        });
    }
    // All-equal variant (when children are queries)
    // TODO: it's workaround mode - should not be used in the future.
    else if (fairShareMode == ELeafFairShare::EQUAL_TO_PARENT) {
        ForEachChild<TQuery>([&](TQuery* query, size_t) {
            if (query->CpuDemand > 0) {
                query->FairShare = FairShare;
            }

            if (auto originalQuery = query->Origin.lock()) {
                originalQuery->SetSnapshot(query->shared_from_this());
            }
        });
    }
    // FIFO variant (when children are queries)
    else {
        // TODO: stable sort children by weight

        auto leftFairShare = FairShare;
        bool allowFairShareOverlimit = (fairShareMode == ELeafFairShare::ALLOW_OVERLIMIT) && (leftFairShare > 0);

        // Give at least 1 fair-share for each demanding child
        ForEachChild<TTreeElement>([&](TTreeElement* child, size_t) -> bool {
            if (!allowFairShareOverlimit && leftFairShare == 0) {
                return true;
            }

            if (child->CpuDemand > 0) {
                child->FairShare = 1;
                if (!allowFairShareOverlimit || leftFairShare > 0) {
                    --leftFairShare;
                }
            }

            return false;
        });
        ForEachChild<TQuery>([&](TQuery* query, size_t) {
            auto demand = query->CpuDemand > 0 ? query->CpuDemand - 1 : 0;
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
        Counters->Satisfaction = counters->Satisfaction;
        Counters->Demand    = counters->Demand;
        Counters->FairShare = counters->FairShare;
    }
}

void TPool::AccountSnapshotDuration(const TDuration& period) {
    if (Counters) {
        const auto fairShare = FairShare * period.MicroSeconds();

        Counters->FairShare->Add(fairShare);

        // TODO: later replace "classical" satisfaction with adjusted one
        if (Satisfaction) {
            Counters->Satisfaction->Set(Satisfaction.value_or(0) * 1'000'000);
        } else {
            Counters->Satisfaction->Set(-1);
        }

        if (auto adjustedFairShare = std::min(fairShare, CpuBurstUsage + CpuBurstThrottle)) {
            float adjustedSatisfaction = CpuBurstUsage / (float)adjustedFairShare;
            Counters->AdjustedSatisfaction->Add(adjustedSatisfaction * period.MicroSeconds());
        } else {
            // by default adjusted satisfaction is always 1.0 - no matter what
            Counters->AdjustedSatisfaction->Add(1.0 * period.MicroSeconds());
        }
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

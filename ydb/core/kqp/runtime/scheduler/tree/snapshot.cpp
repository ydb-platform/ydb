#include "snapshot.h"

#include "dynamic.h" // IWYU pragma: keep

namespace NKikimr::NKqp::NScheduler::NHdrf::NSnapshot {

namespace {

constexpr ui64 MicroCoresPerCore = 1'000'000;

ui64 CeilToCpu(ui64 microCores) {
    return (microCores + MicroCoresPerCore - 1) / MicroCoresPerCore;
}

} // namespace

///////////////////////////////////////////////////////////////////////////////
// TTreeElement
///////////////////////////////////////////////////////////////////////////////

TPool* TTreeElement::GetParent() const {
    return dynamic_cast<TPool*>(Parent);
}

void TTreeElement::AccountSnapshotDuration(TDuration period) {
    ForEachChild<TTreeElement>([&](TTreeElement* child, size_t) {
        child->AccountSnapshotDuration(period);
    });
}

void TTreeElement::UpdateBottomUp(ui64 totalLimit, TDuration period) {
    CpuLimit = Min<ui64>(GetCpuLimit(), totalLimit);

    if (IsPool()) {
        CpuMaxDemand = 0;
        PreciseCpuActualDemand = 0;
        CpuBurstUsage = 0;
        CpuBurstThrottle = 0;
        ForEachChild<TTreeElement>([&](TTreeElement* child, size_t) {
            child->UpdateBottomUp(totalLimit, period);
            CpuMaxDemand += child->CpuMaxDemand;
            PreciseCpuActualDemand += child->PreciseCpuActualDemand;
            CpuBurstUsage += child->CpuBurstUsage;
            CpuBurstThrottle += child->CpuBurstThrottle;
        });

        if (CpuMaxDemand > 0) {
            PreciseCpuActualDemand = Max<ui64>(PreciseCpuActualDemand, MicroCoresPerCore);
        }
    }

    CpuMaxDemand = Min<ui64>(CpuMaxDemand, GetCpuLimit());

    CpuActualDemand = Min<ui64>(CeilToCpu(PreciseCpuActualDemand), GetCpuLimit());
    PreciseCpuActualDemand = Min<ui64>(PreciseCpuActualDemand, CpuActualDemand * MicroCoresPerCore);
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

    // 1st pass: split FairShare by CpuActualDemand - the contested CPU goes to those who really want it.
    ForEachChild<TTreeElement>([&](TTreeElement* child, size_t i) {
        Y_ASSERT(child->CpuMaxDemand >= child->CpuActualDemand);
        children.at(i) = child;
        child->FairShare = 0;
        unsatisfiedDemand.at(i) = child->CpuActualDemand;
    });

    const auto leftFairShare = FillDemand(children, unsatisfiedDemand, FairShare);

    // 2nd pass: give leftFairShare as a headroom up to CpuMaxDemand - to grow before the next snapshot.
    for (size_t i = 0; i < children.size(); ++i) {
        unsatisfiedDemand.at(i) = children.at(i)->CpuMaxDemand - children.at(i)->FairShare;
    }

    FillDemand(children, unsatisfiedDemand, leftFairShare);
}

void TTreeElement::UpdateTopDown() {
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
            if (query->CpuMaxDemand > 0) {
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

void TQuery::UpdateBottomUp(ui64 totalLimit, TDuration period) {
    RawCpuActualDemand = CalculateRawCpuActualDemand(period);

    // The actual demand grows immediately, but falls only when it stays low for two snapshots in a row: otherwise
    // a query which just paused between the bursts of work would lose its share on every other snapshot.
    // It's smoothed only here, and summed up above - so that the actual demand of every element is the sum of its children's.
    PreciseCpuActualDemand = Max(RawCpuActualDemand, PrevRawCpuActualDemand);

    // Every task is able to use at most one CPU - and the departed tasks don't want anything anymore.
    PreciseCpuActualDemand = Min<ui64>(PreciseCpuActualDemand, CpuMaxDemand * MicroCoresPerCore);

    TTreeElement::UpdateBottomUp(totalLimit, period);
}

ui64 TQuery::CalculateRawCpuActualDemand(TDuration period) const {
    // The actual demand is the time the tasks wanted CPU - running or being throttled.
    // The tasks parked on external waits (e.g. network) want nothing and don't contribute.
    ui64 actualDemand = (CpuUsage + CpuThrottle) * MicroCoresPerCore;
    if (period) {
        actualDemand = Max<ui64>(actualDemand, (CpuBurstUsage + CpuBurstThrottle) * MicroCoresPerCore / period.MicroSeconds());
    }

    return actualDemand;
}

///////////////////////////////////////////////////////////////////////////////
// TPool
///////////////////////////////////////////////////////////////////////////////

TPool::TPool(const TPoolId& id, const std::optional<TPoolCounters>& counters, const TStaticAttributes& attrs)
    : NHdrf::TTreeElementBase<ETreeType::SNAPSHOT>(id, attrs)
    , TTreeElement(id, attrs)
    , NHdrf::TPool<ETreeType::SNAPSHOT>(id, attrs)
{
    Counters = counters;
}

void TPool::AccountSnapshotDuration(TDuration period) {
    if (Counters) {
        const auto fairShare = FairShare * period.MicroSeconds();

        Counters->Demand->Set(CpuMaxDemand * 1'000'000);
        Counters->FairShare->Add(fairShare);
        Counters->ActualDemand->Add(CpuActualDemand * period.MicroSeconds());

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

void TRoot::Update(const TRootPtr& previous) {
    const auto period = previous && Timestamp > previous->Timestamp ? Timestamp - previous->Timestamp : TDuration::Zero();

    UpdateBottomUp(TotalLimit, period);

    FairShare = CpuMaxDemand;
    UpdateTopDown();

    if (period) {
        AccountSnapshotDuration(period);
    }
}

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NSnapshot

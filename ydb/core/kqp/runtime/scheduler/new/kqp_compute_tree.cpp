#include "kqp_compute_tree.h"

namespace NKikimr::NKqp::NScheduler::NHdrf {

void TTreeElementBase::AddChild(const TTreeElementPtr& element) {
    const auto usage = element->Usage.load();
    const auto burstUsage = element->BurstUsage.load();
    const auto burstThrottle = element->BurstThrottle.load();

    Children.push_back(element);
    element->Parent = this;

    // In case of detached query we should propagate approximate values to new parents.
    for (NHdrf::TTreeElementBase* parent = this; parent; parent = parent->Parent) {
        parent->Usage += usage;
        parent->BurstUsage += burstUsage;
        parent->BurstThrottle += burstThrottle;
    }
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
            child->FairShare = weightedDemand.at(i) * FairShare / totalWeightedDemand;
        } else {
            child->FairShare = 0;
        }
        child->UpdateTopDown();
    }

    // TODO: distribute resources lost because of integer division.
}

void TTreeElementBase::AccountFairShare(const TDuration& period) {
    for (auto& child : Children) {
        child->AccountFairShare(period);
    }
}

} // namespace NKikimr::NKqp::NScheduler::NHdrf

#include "kqp_rules_include.h"
#include "logical_liveness_helpers.h"

namespace NKikimr {
namespace NKqp {

TNarrowByLivenessStage::TNarrowByLivenessStage()
    : IRBOStage("Narrow by liveness") {
    Props = ERuleProperties::RequireParents | ERuleProperties::RequireNameConstraints;
}

void TNarrowByLivenessStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    Y_UNUSED(ctx);

    for (const auto& iter : root) {
        iter.Current->ClearOutputIUsOverride();
    }

    bool pruned = true;
    while (pruned) {
        pruned = false;
        ComputePlanLiveness(root);
        ComputePlanNameConstraints(root);

        for (const auto& iter : root) {
            if (iter.Current->Kind != EOperator::Map) {
                continue;
            }

            auto map = CastOperator<TOpMap>(iter.Current);
            const auto liveIt = root.PlanProps.LiveOut.find(map.get());
            if (liveIt == root.PlanProps.LiveOut.end()) {
                continue;
            }

            auto newElements = KeepLiveMapElements(map, liveIt->second, root.PlanProps);
            if (newElements.size() == map->MapElements.size()) {
                continue;
            }

            auto oldElements = std::move(map->MapElements);
            map->MapElements = std::move(newElements);
            if (!CanExposeToParents(map.get(), root.PlanProps)) {
                map->MapElements = std::move(oldElements);
                continue;
            }

            pruned = true;
        }

        for (const auto& iter : root) {
            if (iter.Current->Kind != EOperator::Aggregate) {
                continue;
            }

            auto aggregate = CastOperator<TOpAggregate>(iter.Current);
            const auto liveIt = root.PlanProps.LiveOut.find(aggregate.get());
            if (liveIt == root.PlanProps.LiveOut.end()) {
                continue;
            }

            const auto liveOutput = KeepLiveColumns(aggregate->GetOutputIUs(), liveIt->second);
            pruned |= PruneAggregateTraits(aggregate, liveOutput);
        }
    }

    ComputePlanLiveness(root);

    THashMap<IOperator*, TVector<TInfoUnit>> liveOutputs;
    for (const auto& iter : root) {
        const auto liveIt = root.PlanProps.LiveOut.find(iter.Current.get());
        if (liveIt == root.PlanProps.LiveOut.end()) {
            continue;
        }

        auto liveOut = liveIt->second;
        if (iter.Current->Kind == EOperator::Sort) {
            for (const auto& sortElement : CastOperator<TOpSort>(iter.Current)->SortElements) {
                AddLiveColumn(liveOut, sortElement.SortColumn);
            }
        }

        liveOutputs[iter.Current.get()] = KeepLiveColumns(iter.Current->GetOutputIUs(), liveOut);
    }

    for (const auto& iter : root) {
        auto op = iter.Current;
        const auto outputIt = liveOutputs.find(op.get());
        if (outputIt == liveOutputs.end()) {
            continue;
        }

        const auto& liveOutput = outputIt->second;
        if (op->Kind == EOperator::Source) {
            NarrowReadColumns(CastOperator<TOpRead>(op), liveOutput);
            continue;
        }

        if (op->Kind == EOperator::Aggregate) {
            PruneAggregateTraits(CastOperator<TOpAggregate>(op), liveOutput);
        }

        if (!CanOverrideOutput(op->Kind)) {
            continue;
        }

        if (op->GetOutputIUs() != liveOutput) {
            op->SetOutputIUsOverride(liveOutput);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr

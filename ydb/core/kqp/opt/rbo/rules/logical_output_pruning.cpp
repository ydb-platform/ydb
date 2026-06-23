#include "kqp_rules_include.h"
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

namespace NKikimr {
namespace NKqp {

TLogicalOutputPruningStage::TLogicalOutputPruningStage()
    : IRBOStage("Prune dead logical outputs") {
    Props = ERuleProperties::RequireParents | ERuleProperties::RequireNameConstraints;
}

void TLogicalOutputPruningStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    Y_UNUSED(ctx);

    bool pruned = true;
    THashMap<IOperator*, TVector<TInfoUnit>> cacheOnlyOutputs;
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
            const auto inputLiveIt = root.PlanProps.LiveOut.find(map->GetInput().get());
            TVector<TInfoUnit> inputOutput = map->GetInput()->GetOutputIUs();
            if (inputLiveIt != root.PlanProps.LiveOut.end()) {
                inputOutput = KeepLiveColumns(inputOutput, inputLiveIt->second);
            }

            auto newOutput = BuildMapOutput(inputOutput, newElements);
            if (newElements.size() == map->MapElements.size() && newOutput == map->GetOutputIUs()) {
                continue;
            }

            if (!CanExposeOutput(map, newOutput, root.PlanProps)) {
                continue;
            }

            const bool elementsChanged = newElements.size() != map->MapElements.size();
            map->MapElements = std::move(newElements);
            auto [cacheOnlyIt, inserted] = cacheOnlyOutputs.emplace(map.get(), newOutput);
            if (elementsChanged || inserted || cacheOnlyIt->second != newOutput) {
                cacheOnlyIt->second = newOutput;
                pruned = true;
            }
            map->Props.OutputIUs = std::move(newOutput);
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

    for (const auto& iter : root) {
        auto op = iter.Current;
        const auto liveIt = root.PlanProps.LiveOut.find(iter.Current.get());
        if (liveIt == root.PlanProps.LiveOut.end()) {
            continue;
        }

        const auto liveOutput = KeepLiveColumns(op->GetOutputIUs(), liveIt->second);
        if (op->Kind == EOperator::Source) {
            NarrowReadColumns(CastOperator<TOpRead>(op), liveOutput);
            continue;
        }

        if (op->Kind == EOperator::Aggregate) {
            PruneAggregateTraits(CastOperator<TOpAggregate>(op), liveOutput);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr

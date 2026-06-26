#include "kqp_rules_include.h"
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

namespace NKikimr {
namespace NKqp {

TLogicalOutputPruningStage::TLogicalOutputPruningStage()
    : IRBOStage("Prune dead logical outputs") {
    Props = ERuleProperties::RequireParents | ERuleProperties::RequireLiveness | ERuleProperties::RequireNameConstraints;
}

void TLogicalOutputPruningStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    bool pruned = true;
    while (pruned) {
        pruned = false;
        ComputeRequiredProps(root, Props, ctx, StageName);

        for (const auto& iter : root) {
            if (iter.Current->Kind != EOperator::Map) {
                continue;
            }

            auto map = CastOperator<TOpMap>(iter.Current);
            const auto& liveOut = GetLiveOut(map.get());
            auto newElements = KeepLiveMapElements(map, liveOut);
            if (newElements.size() == map->MapElements.size()) {
                continue;
            }

            auto newOutput = BuildMapOutput(map, newElements);
            if (MakeInfoUnitSet(newOutput).size() != newOutput.size() ||
                !IUSetIntersect(newOutput, GetForbidden(map.get())).empty()) {
                continue;
            }

            map->MapElements = std::move(newElements);
            map->Props.OutputIUs = std::move(newOutput);
            pruned = true;
        }

        for (const auto& iter : root) {
            if (iter.Current->Kind != EOperator::Aggregate) {
                continue;
            }

            auto aggregate = CastOperator<TOpAggregate>(iter.Current);
            const auto& liveOut = GetLiveOut(aggregate.get());
            const auto liveOutput = KeepLiveColumns(aggregate->GetOutputIUs(), liveOut);
            pruned |= PruneAggregateTraits(aggregate, liveOutput);
        }
    }

    ComputeRequiredProps(root, Props, ctx, StageName);

    for (const auto& iter : root) {
        auto op = iter.Current;
        const auto& liveOut = GetLiveOut(iter.Current.get());
        const auto liveOutput = KeepLiveColumns(op->GetOutputIUs(), liveOut);
        if (op->Kind == EOperator::Source) {
            NarrowReadColumns(CastOperator<TOpRead>(op), liveOutput);
            continue;
        }

        if (op->Kind == EOperator::Aggregate) {
            PruneAggregateTraits(CastOperator<TOpAggregate>(op), liveOutput);
        }
    }

    root.ComputeOutputIUsSubtree();
}

} // namespace NKqp
} // namespace NKikimr

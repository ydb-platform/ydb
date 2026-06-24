#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

namespace NKikimr {
namespace NKqp {

bool TPruneDeadMapElementsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto map = CastOperator<TOpMap>(input);
    const auto liveIt = props.LiveOut.find(map.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    // If we need to keep key columns, add them to keep list
    TInfoUnitSet keepKeyColumns;
    if (!PruneKeyColumns) {
        for (auto column : input->Props.Metadata->KeyColumns) {
            keepKeyColumns.insert(column);
        }
    }

    auto newElements = KeepLiveMapElements(map, liveIt->second, props, keepKeyColumns);

    if (newElements.empty()) {
        const auto& replacementOutput = map->GetInput()->GetOutputIUs();
        if (!CanReplaceOutputInParents(map, replacementOutput, props)) {
            return false;
        }
        input = map->GetInput();
    } else {
        if (newElements.size() == map->MapElements.size()) {
            return false;
        }

        auto newOutput = BuildMapOutput(map, newElements);
        if (!CanReplaceOutputInParents(map, newOutput, props)) {
            return false;
        }
        map->MapElements = std::move(newElements);
        map->Props.OutputIUs = std::move(newOutput);
    }

    return true;
}

bool TPruneDeadReadColumnsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Source) {
        return false;
    }

    auto read = CastOperator<TOpRead>(input);
    const auto liveIt = props.LiveOut.find(read.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    // If we need to keep key columns, add them to keep list
    TInfoUnitSet keepKeyColumns;
    if (!PruneKeyColumns) {
        for (auto column : input->Props.Metadata->KeyColumns) {
            keepKeyColumns.insert(column);
        }
    }

    const auto liveOutput = KeepLiveColumns(read->GetOutputIUs(), liveIt->second, keepKeyColumns);
    return NarrowReadColumns(read, liveOutput);
}

bool TPruneDeadAggregateTraitsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Aggregate || CastOperator<TOpAggregate>(input)->IsDistinctAll()) {
        return false;
    }

    auto aggregate = CastOperator<TOpAggregate>(input);
    const auto liveIt = props.LiveOut.find(aggregate.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    // Key columns will be preserved in the aggregate anyway
    const auto liveOutput = KeepLiveColumns(aggregate->GetOutputIUs(), liveIt->second);
    return PruneAggregateTraits(aggregate, liveOutput);
}

} // namespace NKqp
} // namespace NKikimr

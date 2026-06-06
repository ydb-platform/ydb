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

    auto newElements = KeepLiveMapElements(map, liveIt->second, props);
    if (newElements.size() == map->MapElements.size()) {
        return false;
    }

    if (newElements.empty()) {
        if (!CanReplaceInParents(map, map->GetInput(), props)) {
            return false;
        }
        input = map->GetInput();
    } else {
        auto oldElements = std::move(map->MapElements);
        map->MapElements = std::move(newElements);
        if (!CanExposeToParents(map.get(), props)) {
            map->MapElements = std::move(oldElements);
            return false;
        }
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

    const auto liveOutput = KeepLiveColumns(read->GetOutputIUs(), liveIt->second);
    return NarrowReadColumns(read, liveOutput);
}

bool TPruneDeadAggregateTraitsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Aggregate) {
        return false;
    }

    auto aggregate = CastOperator<TOpAggregate>(input);
    const auto liveIt = props.LiveOut.find(aggregate.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    const auto liveOutput = KeepLiveColumns(aggregate->GetOutputIUs(), liveIt->second);
    return PruneAggregateTraits(aggregate, liveOutput);
}

} // namespace NKqp
} // namespace NKikimr

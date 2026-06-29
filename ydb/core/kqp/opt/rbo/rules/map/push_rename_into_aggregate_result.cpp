#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

#include <algorithm>

namespace NKikimr {
namespace NKqp {

namespace {

bool ProducesAggregateResult(const TIntrusivePtr<TOpAggregate>& aggregate, const TInfoUnit& iu) {
    return std::any_of(aggregate->AggregationTraitsList.begin(), aggregate->AggregationTraitsList.end(), [&iu](const TOpAggregationTraits& traits) {
        return traits.ResultColName == iu;
    });
}

} // anonymous namespace

bool TPushRenameIntoAggregateResultRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto candidate = NMapRules::FindRenameCandidate(topMap);
    if (!candidate || !NMapRules::CanStartLocalRenamePush(topMap, *candidate)) {
        return false;
    }

    if (topMap->GetInput()->Kind != EOperator::Aggregate) {
        return false;
    }

    auto aggregate = CastOperator<TOpAggregate>(topMap->GetInput());
    if (!aggregate->IsSingleConsumer() || !ProducesAggregateResult(aggregate, candidate->From)) {
        return false;
    }

    for (auto& traits : aggregate->AggregationTraitsList) {
        if (traits.ResultColName == candidate->From) {
            traits.ResultColName = candidate->To;
            break;
        }
    }

    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr

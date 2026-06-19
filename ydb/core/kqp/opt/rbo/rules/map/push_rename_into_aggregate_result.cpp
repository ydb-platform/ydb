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
    const auto candidate = NMapRules::FindRenameCandidate(topMap, props);
    if (!candidate || !NMapRules::CanStartLocalRenamePush(topMap, *candidate, props)) {
        return false;
    }

    if (topMap->GetInput()->Kind != EOperator::Aggregate) {
        return false;
    }

    auto aggregate = CastOperator<TOpAggregate>(topMap->GetInput());
    if (!aggregate->IsSingleConsumer() || !ProducesAggregateResult(aggregate, candidate->From) ||
        !NMapRules::CanRenameOutput(aggregate, candidate->From, candidate->To, props)) {
        return false;
    }

    const auto oldTraits = aggregate->AggregationTraitsList;
    for (auto& traits : aggregate->AggregationTraitsList) {
        if (traits.ResultColName == candidate->From) {
            traits.ResultColName = candidate->To;
            break;
        }
    }

    if (HasOutputConflicts(aggregate->GetOutputIUs())) {
        aggregate->AggregationTraitsList = oldTraits;
        return false;
    }

    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr

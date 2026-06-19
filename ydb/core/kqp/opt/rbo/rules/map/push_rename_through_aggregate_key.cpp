#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool ProducesAggregateKey(const TIntrusivePtr<TOpAggregate>& aggregate, const TInfoUnit& iu) {
    return ContainsInfoUnit(aggregate->KeyColumns, iu);
}

} // anonymous namespace

bool TPushRenameThroughAggregateKeyRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
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
    if (!aggregate->IsSingleConsumer() || !ProducesAggregateKey(aggregate, candidate->From) ||
        !NMapRules::CanRenameOutput(aggregate, candidate->From, candidate->To, props)) {
        return false;
    }

    const auto oldInput = aggregate->GetInput();
    const auto oldKeys = aggregate->KeyColumns;
    const auto oldTraits = aggregate->AggregationTraitsList;
    auto pushedMap = MakeIntrusive<TOpMap>(oldInput, topMap->Pos, TVector<TMapElement>{NMapRules::MakeRenameElement(*candidate, topMap)});
    if (HasOutputConflicts(pushedMap->GetOutputIUs())) {
        return false;
    }

    aggregate->SetInput(pushedMap);
    aggregate->RenameIUs({{candidate->From, candidate->To}}, ctx.ExprCtx);
    if (HasOutputConflicts(aggregate->GetOutputIUs())) {
        aggregate->SetInput(oldInput);
        aggregate->KeyColumns = oldKeys;
        aggregate->AggregationTraitsList = oldTraits;
        return false;
    }

    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr

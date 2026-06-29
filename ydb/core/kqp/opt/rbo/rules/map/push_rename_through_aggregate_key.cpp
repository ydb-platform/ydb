#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool ProducesAggregateKey(const TIntrusivePtr<TOpAggregate>& aggregate, const TInfoUnit& iu) {
    return ContainsInfoUnit(aggregate->KeyColumns, iu);
}

void RenameAggregate(
    TVector<TInfoUnit>& keyColumns,
    TVector<TOpAggregationTraits>& traitsList,
    const TInfoUnit& from,
    const TInfoUnit& to)
{
    for (auto& key : keyColumns) {
        if (key == from) {
            key = to;
        }
    }
    for (auto& traits : traitsList) {
        if (traits.OriginalColName == from) {
            traits.OriginalColName = to;
        }
        if (traits.ResultColName == from) {
            traits.ResultColName = to;
        }
    }
}

} // anonymous namespace

bool TPushRenameThroughAggregateKeyRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
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
    if (!aggregate->IsSingleConsumer() || !ProducesAggregateKey(aggregate, candidate->From)) {
        return false;
    }

    const auto oldInput = aggregate->GetInput();
    const TVector<TMapElement> pushedElements{NMapRules::MakeRenameElement(*candidate, topMap)};
    auto pushedMap = MakeIntrusive<TOpMap>(oldInput, topMap->Pos, pushedElements);
    aggregate->SetInput(pushedMap);
    RenameAggregate(aggregate->KeyColumns, aggregate->AggregationTraitsList, candidate->From, candidate->To);
    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr

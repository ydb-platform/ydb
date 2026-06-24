#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool ProducesAggregateKey(const TIntrusivePtr<TOpAggregate>& aggregate, const TInfoUnit& iu) {
    return ContainsInfoUnit(aggregate->KeyColumns, iu);
}

TVector<TInfoUnit> BuildAggregateOutput(
    const TVector<TInfoUnit>& keyColumns,
    const TVector<TOpAggregationTraits>& traitsList,
    bool distinctAll)
{
    TVector<TInfoUnit> output;
    if (!distinctAll) {
        output = keyColumns;
    }
    for (const auto& traits : traitsList) {
        output.push_back(traits.ResultColName);
    }
    return output;
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
    const TVector<TMapElement> pushedElements{NMapRules::MakeRenameElement(*candidate, topMap)};
    const auto pushedOutput = BuildMapOutput(oldInput->GetOutputIUs(), pushedElements);
    if (HasOutputConflicts(pushedOutput)) {
        return false;
    }

    auto newKeys = aggregate->KeyColumns;
    auto newTraits = aggregate->AggregationTraitsList;
    RenameAggregate(newKeys, newTraits, candidate->From, candidate->To);
    const auto output = BuildAggregateOutput(newKeys, newTraits, aggregate->IsDistinctAll());
    if (HasOutputConflicts(output) || !NMapRules::CanFinishRenamePush(topMap, *candidate, output, props)) {
        return false;
    }

    auto pushedMap = MakeIntrusive<TOpMap>(oldInput, topMap->Pos, pushedElements);
    pushedMap->Props.OutputIUs = pushedOutput;
    aggregate->SetInput(pushedMap);
    aggregate->KeyColumns = std::move(newKeys);
    aggregate->AggregationTraitsList = std::move(newTraits);
    aggregate->Props.OutputIUs = output;
    return NMapRules::FinishRenamePush(input, topMap, *candidate, output, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr

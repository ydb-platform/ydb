#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {
const THashSet<TString> AllowedAggFunction{"sum", "min", "max", "count", "avg", "variance_1_1", "some", "distinct"};

bool IsValidConnectionToPushAggregation(const TConnection& connection) {
    return IsConnection<TUnionAllConnection>(connection) || IsConnection<TShuffleConnection>(connection) || IsConnection<TMapConnection>(connection);
}

bool CanPushAggregateToStage(const TIntrusivePtr<TOpAggregate>& aggregate, const TIntrusivePtr<IOperator>& input, TPlanProps& props) {
    const auto aggregateStageId = *aggregate->Props.StageId;
    const auto inputStageId = *input->Props.StageId;
    if (aggregateStageId == inputStageId) {
        return false;
    }
    auto* edge = props.StageGraph.FindUniqueEdge(props.GetStage(*input), props.GetStage(*aggregate));
    if (!edge || !IsValidConnectionToPushAggregation(edge->Connection())) {
        return false;
    }

    // A port qualifies even over a row-storage read: its aggregate lowers
    // inside the Switch branch, which BuildSwitch places into that read's
    // stage program.
    return (input->GetKind() != EOperator::Source || CastOperator<TOpRead>(input)->GetTableStorageType() == NYql::EStorageType::ColumnStorage);
}

bool AggregationTraitsAreValidForPropagation(const TAggregationIUs& aggregationTraitsList) {
    for (const auto& [output, aggTraits] : aggregationTraitsList.Items()) {
        if (!AllowedAggFunction.contains(aggTraits.AggFunction)) {
            return false;
        }
    }
    return true;
}

bool IsSuitableToPropagateAggregateThroughStage(const TIntrusivePtr<IOperator>& input) {
    if (input->GetKind() != EOperator::Aggregate) {
        return false;
    }

    const auto aggregate = CastOperator<TOpAggregate>(input);
    const auto& aggTraits = aggregate->GetAggregationTraits();

    return aggregate->GetAggregationPhase() != EOpPhase::Final && AggregationTraitsAreValidForPropagation(aggTraits);
}

std::pair<TString, TString> GetAggFunctions(const TString& aggFunc) {
    if (aggFunc == "min" || aggFunc == "max" || aggFunc == "sum" || aggFunc == "avg" || aggFunc == "variance_1_1" || aggFunc == "some" ||
        aggFunc == "distinct") {
        return std::make_pair(aggFunc, aggFunc);
    }
    if (aggFunc == "count") {
        return std::make_pair("count", "sum");
    }
    Y_ENSURE(false, "Aggregation function is not supported for splitting.");
}

TIntrusivePtr<TOpAggregate> EmitFinalAndIntermediateAggregates(const TIntrusivePtr<TOpAggregate>& aggregate, TInfoUnitRegistry& registry) {
    const auto pos = aggregate->Pos;
    const auto props = aggregate->Props;
    const auto& aggregationTraitsList = aggregate->GetAggregationTraits();
    const auto& aggKeys = aggregate->GetKeyColumns();
    const auto distinctAll = aggregate->IsDistinctAll();

    TAggregationIUs intermediateTraits;
    TAggregationIUs finalTraits;
    TOrderedIUs<> distKeys;

    // Here we want to split aggregate to final and intermediate.
    for (const auto output : aggregationTraitsList.Keys()) {
        const auto& originalTraits = *aggregationTraitsList.Find(output);
        const auto intermediateId = registry.AddGenerated("intermediate_agg");
        const auto [interAggFunc, finalAggFunc] = GetAggFunctions(originalTraits.AggFunction);
        intermediateTraits.Add(intermediateId, TOpAggregationTraits{originalTraits.Input, interAggFunc});
        finalTraits.Add(output, TOpAggregationTraits{intermediateId, finalAggFunc});

        if (distinctAll) {
            distKeys.Append(intermediateId);
        }
    }

    const auto intermediate = MakeIntrusive<TOpAggregate>(aggregate->GetInput(), intermediateTraits, aggKeys, EOpPhase::Intermediate, distinctAll, props, pos);
    auto finalProps = props;
    finalProps.StageInputEdges.clear();
    return MakeIntrusive<TOpAggregate>(intermediate, finalTraits, distinctAll ? distKeys : aggKeys, EOpPhase::Final, distinctAll, finalProps, pos);
}

} // namespace

bool TPropagateAggregateThroughStageRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Aggregate;
}

TIntrusivePtr<IOperator> TPropagateAggregateThroughStageRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (!IsSuitableToPropagateAggregateThroughStage(input)) {
        return input;
    }

    const auto aggregate = CastOperator<TOpAggregate>(input);
    if (aggregate->GetAggregationPhase() == EOpPhase::Undefined) {
        return EmitFinalAndIntermediateAggregates(aggregate, props.InfoUnitRegistry);
    }

    const auto aggInput = aggregate->GetInput();
    if (CanPushAggregateToStage(aggregate, aggInput, props)) {
        const auto inputStageId = *aggInput->Props.StageId;
        auto* edge = props.GetStageInputEdge(*aggregate, 0);
        Y_ENSURE(edge, "Aggregate has no stage input");
        auto opProps = aggregate->Props;
        opProps.StageId = inputStageId;
        opProps.StageInputEdges.clear();

        TIntrusivePtr<TConnection> connection;
        if (CanEliminateAggregateShuffle(*aggregate, ctx)) {
            connection = MakeIntrusive<TMapConnection>(edge->OutputIndex());
        } else if (!aggregate->GetKeyColumns().Items().empty()) {
            connection = MakeIntrusive<TShuffleConnection>(GetAggregateOutputShuffle(*aggregate), edge->OutputIndex());
        } else {
            connection = MakeIntrusive<TUnionAllConnection>(edge->OutputIndex());
        }

        edge->ReplaceConnection(connection);
        return MakeIntrusive<TOpAggregate>(aggInput, aggregate->GetAggregationTraits(), aggregate->GetKeyColumns(), EOpPhase::Intermediate,
                                           aggregate->IsDistinctAll(), opProps, aggregate->Pos);
    }

    return input;
}
} // namespace NKqp
} // namespace NKikimr

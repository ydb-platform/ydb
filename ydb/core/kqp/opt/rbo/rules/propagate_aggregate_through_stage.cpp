#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {
const THashSet<TString> AllowedAggFunction{"sum", "min", "max", "count", "avg", "variance_1_1", "some", "distinct"};

bool IsValidConnectionToPushAggregation(const TIntrusivePtr<TConnection>& connection) {
    return IsConnection<TUnionAllConnection>(connection) || IsConnection<TShuffleConnection>(connection) || IsConnection<TMapConnection>(connection);
}

bool CanPushAggregateToStage(const TIntrusivePtr<TOpAggregate>& aggregate, const TIntrusivePtr<IOperator>& input, TPlanProps& props) {
    const auto aggregateStageId = *aggregate->Props.StageId;
    const auto inputStageId = *input->Props.StageId;
    if (aggregateStageId == inputStageId || input->Kind == EOperator::Replicate) {
        return false;
    }
    const auto connection = props.StageGraph.GetConnections(inputStageId, aggregateStageId);
    if (connection.size() > 1 || !IsValidConnectionToPushAggregation(connection.front())) {
        return false;
    }

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
    return MakeIntrusive<TOpAggregate>(intermediate, finalTraits, distinctAll ? distKeys : aggKeys, EOpPhase::Final, distinctAll, props, pos);
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
        const auto aggStageId = *aggregate->Props.StageId;
        const auto inputStageId = *aggInput->Props.StageId;
        const auto connections = props.StageGraph.GetConnections(inputStageId, aggStageId);
        Y_ENSURE(connections.size() == 1, "Invalid number of connections.");
        const auto outputIndex = connections.front()->GetOutputIndex();
        auto opProps = aggregate->Props;
        opProps.StageId = inputStageId;

        TIntrusivePtr<TConnection> connection;
        if (CanEliminateAggregateShuffle(*aggregate, ctx)) {
            connection = MakeIntrusive<TMapConnection>(outputIndex);
        } else if (!aggregate->GetKeyColumns().Items().empty()) {
            TOrderedIUs<> shuffleByKeys;
            if (aggregate->IsDistinctAll()) {
                for (const auto output : aggregate->GetAggregationTraits().Keys()) {
                    shuffleByKeys.Append(output);
                }
            } else {
                shuffleByKeys = aggregate->GetKeyColumns();
            }
            connection = MakeIntrusive<TShuffleConnection>(shuffleByKeys, outputIndex);
        } else {
            connection = MakeIntrusive<TUnionAllConnection>(outputIndex);
        }

        props.StageGraph.UpdateConnection(inputStageId, aggStageId, connection);
        return MakeIntrusive<TOpAggregate>(aggInput, aggregate->GetAggregationTraits(), aggregate->GetKeyColumns(), EOpPhase::Intermediate,
                                           aggregate->IsDistinctAll(), opProps, aggregate->Pos);
    }

    return input;
}
} // namespace NKqp
} // namespace NKikimr

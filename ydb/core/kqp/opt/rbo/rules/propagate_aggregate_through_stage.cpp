#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {
const THashSet<TString> AllowedAggFunction{"sum", "min", "max", "count"};

bool IsValidConnectionToPushAggregation(const TIntrusivePtr<TConnection>& connection) {
    return IsConnection<TUnionAllConnection>(connection) || IsConnection<TShuffleConnection>(connection);
}

bool CanPushAggregateToStage(const TIntrusivePtr<TOpAggregate>& aggregate, const TIntrusivePtr<IOperator>& input, TPlanProps& props) {
    const auto aggregateStageId = *aggregate->Props.StageId;
    const auto inputStageId = *input->Props.StageId;
    if (aggregateStageId == inputStageId || !input->IsSingleConsumer()) {
        return false;
    }
    const auto connection = props.StageGraph.GetConnections(inputStageId, aggregateStageId);
    if (connection.size() > 1 || !IsValidConnectionToPushAggregation(connection.front())) {
        return false;
    }

    return (input->GetKind() != EOperator::Source || CastOperator<TOpRead>(input)->GetTableStorageType() == NYql::EStorageType::ColumnStorage);
}

bool AggregationTraitsAreValidForPropagation(const TVector<TOpAggregationTraits>& aggregationTraitsList) {
    for (const auto& aggTraits : aggregationTraitsList) {
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
    const auto distinctAll = aggregate->IsDistinctAll();

    return aggregate->GetAggregationPhase() != EOpPhase::Final && AggregationTraitsAreValidForPropagation(aggTraits) && !distinctAll;
}

std::pair<TString, TString> GetAggFunctions(const TString& aggFunc) {
    if (aggFunc == "min" || aggFunc == "max" || aggFunc == "sum") {
        return std::make_pair(aggFunc, aggFunc);
    }
    if (aggFunc == "count") {
        return std::make_pair("count", "sum");
    }
    Y_ENSURE(false, "Aggregation function is not supported for splitting.");
}

TIntrusivePtr<TOpAggregate> EmitFinalAndIntermediateAggregates(const TIntrusivePtr<TOpAggregate>& aggregate) {
    const auto pos = aggregate->Pos;
    const auto props = aggregate->Props;
    const auto& aggregationTraitsList = aggregate->GetAggregationTraits();
    const auto& aggKeys = aggregate->GetKeyColumns();
    const auto distinctAll = aggregate->IsDistinctAll();

    TVector<TOpAggregationTraits> intermediateTraits;
    TVector<TOpAggregationTraits> finalTraits;
    // Here we want to split aggregate to final and intermediate.
    for (const auto& originalTraits : aggregationTraitsList) {
        const auto& originalColName = originalTraits.OriginalColName;
        const auto& aggFunc = originalTraits.AggFunction;
        const auto& resultColName = originalTraits.ResultColName;
        const auto newIntermediateName = TInfoUnit("__intermediate_" + resultColName.GetFullName());
        const auto [interAggFunc, finalAggFunc] = GetAggFunctions(aggFunc);
        intermediateTraits.emplace_back(originalColName, interAggFunc, newIntermediateName);
        finalTraits.emplace_back(newIntermediateName, finalAggFunc, resultColName);
    }

    const auto intermediate = MakeIntrusive<TOpAggregate>(aggregate->GetInput(), intermediateTraits, aggKeys, EOpPhase::Intermediate, distinctAll, props, pos);
    return MakeIntrusive<TOpAggregate>(intermediate, finalTraits, aggKeys, EOpPhase::Final, distinctAll, props, pos);
}

} // namespace

TIntrusivePtr<IOperator> TPropagateAggregateThroughStageRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (!IsSuitableToPropagateAggregateThroughStage(input)) {
        return input;
    }

    const auto aggregate = CastOperator<TOpAggregate>(input);
    if (aggregate->GetAggregationPhase() == EOpPhase::Undefined) {
        return EmitFinalAndIntermediateAggregates(aggregate);
    }

    const auto aggInput = aggregate->GetInput();
    if (CanPushAggregateToStage(aggregate, aggInput, props)) {
        const auto aggStageId = *aggregate->Props.StageId;
        const auto inputStageId = *aggInput->Props.StageId;
        auto opProps = aggregate->Props;
        opProps.StageId = inputStageId;
        TIntrusivePtr<TConnection> connection = MakeIntrusive<TShuffleConnection>(aggregate->GetKeyColumns());
        if (aggregate->GetKeyColumns().empty()) {
            connection = MakeIntrusive<TUnionAllConnection>();
        }

        props.StageGraph.UpdateConnection(inputStageId, aggStageId, connection);
        return MakeIntrusive<TOpAggregate>(aggInput, aggregate->GetAggregationTraits(), aggregate->GetKeyColumns(), EOpPhase::Intermediate,
                                           aggregate->IsDistinctAll(), opProps, aggregate->Pos);
    }

    return input;
}
} // namespace NKqp
} // namespace NKikimr

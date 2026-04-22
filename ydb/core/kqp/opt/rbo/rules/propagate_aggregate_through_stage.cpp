#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {
const THashSet<TString> AllowedAggFunction{"sum", "min", "max"};

bool CanPushAggregateToStage(const TIntrusivePtr<TOpAggregate>& aggregate, const TIntrusivePtr<IOperator>& input) {
    const auto aggregateStageId = *aggregate->Props.StageId;
    const auto inputStageId = *input->Props.StageId;
    // FIXME: Currently we cannot push to source stage.
    return aggregateStageId != inputStageId && input->IsSingleConsumer() && input->GetKind() != EOperator::Source;
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

    return aggregate->GetAggregationPhase() != EOpPhase::Final && AggregationTraitsAreValidForPropagation(aggTraits) && !distinctAll &&
           !aggregate->GetKeyColumns().empty();
}

TIntrusivePtr<TOpAggregate> EmitFinalAndIntermediateAggregates(const TIntrusivePtr<TOpAggregate>& aggregate, TRBOContext& ctx) {
    const auto pos = aggregate->Pos;
    const auto props = aggregate->Props;
    const auto& aggregationTraitsList = aggregate->GetAggregationTraits();
    const auto& aggKeys = aggregate->GetKeyColumns();
    const auto distinctAll = aggregate->IsDistinctAll();
    std::optional<i64> memLimit;
    if (auto memLimitSetting = ctx.KqpCtx.Config->_KqpYqlCombinerMemoryLimit.Get()) {
        memLimit = -i64(*memLimitSetting);
    }

    TVector<TOpAggregationTraits> intermediateTraits;
    TVector<TOpAggregationTraits> finalTraits;
    // Here we want to split aggregate to final and intermediate.
    for (const auto& originalTraits: aggregationTraitsList) {
        const auto& originalColName = originalTraits.OriginalColName;
        const auto& aggFunc = originalTraits.AggFunction;
        const auto& resultColName = originalTraits.ResultColName;
        const auto newIntermediateName = TInfoUnit("__inter" + resultColName.GetFullName());
        intermediateTraits.emplace_back(originalColName, aggFunc, newIntermediateName);
        finalTraits.emplace_back(newIntermediateName, aggFunc, resultColName);
    }

    const auto intermediate =
        MakeIntrusive<TOpAggregate>(aggregate->GetInput(), intermediateTraits, aggKeys, EOpPhase::Intermediate, distinctAll, memLimit, props, pos);
    return MakeIntrusive<TOpAggregate>(intermediate, finalTraits, aggKeys, EOpPhase::Final, distinctAll, std::nullopt, props, pos);
}

} // namespace

TIntrusivePtr<IOperator> TPropagateAggregateThroughStageRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (!IsSuitableToPropagateAggregateThroughStage(input)) {
        return input;
    }

    const auto aggregate = CastOperator<TOpAggregate>(input);
    if (aggregate->GetAggregationPhase() == EOpPhase::Undefined) {
        return EmitFinalAndIntermediateAggregates(aggregate, ctx);
    }

    const auto aggInput = aggregate->GetInput();
    if (CanPushAggregateToStage(aggregate, aggInput)) {
        auto props = aggregate->Props;
        props.StageId = aggInput->Props.StageId;
        return MakeIntrusive<TOpAggregate>(aggInput, aggregate->GetAggregationTraits(), aggregate->GetKeyColumns(), EOpPhase::Intermediate,
                                           aggregate->IsDistinctAll(), aggregate->GetMemoryLimit(), props, aggregate->Pos);
    }

    return input;
}
} // namespace NKqp
} // namespace NKikimr
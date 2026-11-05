#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

bool CanPushLimitToRead(const TIntrusivePtr<IOperator>& input) {
    if (input->GetKind() != EOperator::Source) {
        return false;
    }
    const auto read = CastOperator<TOpRead>(input);
    return !read->Limit && read->GetTableStorageType() == NYql::EStorageType::ColumnStorage;
}

bool CanPushLimitOverInput(const TIntrusivePtr<TOpLimit>& limit, const TIntrusivePtr<IOperator>& input) {
    const auto kind = input->GetKind();
    return (kind == EOperator::Map && limit->Props.StageId == input->Props.StageId);
}

bool CanPushLimitToStage(const TIntrusivePtr<TOpLimit>& limit, const TIntrusivePtr<IOperator>& input) {
    return !(limit->Props.StageId == input->Props.StageId || input->Kind == EOperator::Replicate ||
             (input->GetKind() == EOperator::Source && CastOperator<TOpRead>(input)->GetTableStorageType() == NYql::EStorageType::RowStorage));
}

bool IsSuitableToPropagateLimitThroughStage(const TIntrusivePtr<IOperator>& input) {
    if (input->GetKind() != EOperator::Limit) {
        return false;
    }
    return CastOperator<TOpLimit>(input)->GetLimitPhase() != EOpPhase::Final;
}

TIntrusivePtr<TOpLimit> EmitFinalAndIntermediateLimits(const TIntrusivePtr<TOpLimit>& limit, TExprContext& ctx, TPlanProps& planProps) {
    const auto pos = limit->Pos;
    const auto props = limit->Props;
    const auto input = limit->GetInput();
    // A limit with an offset over a sort or another limit: its input already holds the rows it needs.
    if (limit->HasOffset() && (input->GetKind() == EOperator::Sort || input->GetKind() == EOperator::Limit)) {
        return MakeIntrusive<TOpLimit>(input, pos, props, limit->GetLimitCond(), limit->GetOffsetCond(), EOpPhase::Final);
    }
    // The intermediate limit also keeps the rows that the final limit's offset skips.
    const auto intermediateLimit = MakeIntrusive<TOpLimit>(input, pos, props, limit->GetInputRowsLimit(ctx, planProps), EOpPhase::Intermediate);
    auto finalProps = props;
    finalProps.StageInputEdges.clear();
    return MakeIntrusive<TOpLimit>(intermediateLimit, pos, finalProps, limit->GetLimitCond(), limit->GetOffsetCond(), EOpPhase::Final);
}

} // namespace

bool TPropagateLimitThroughStageRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Limit;
}

TIntrusivePtr<IOperator> TPropagateLimitThroughStageRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (!IsSuitableToPropagateLimitThroughStage(input)) {
        return input;
    }

    const auto limit = CastOperator<TOpLimit>(input);
    // Split one limit on final and intermediate, we will later propagate intermediate through stages.
    if (limit->GetLimitPhase() == EOpPhase::Undefined) {
        return EmitFinalAndIntermediateLimits(limit, ctx.ExprCtx, props);
    }
    Y_ENSURE(limit->GetLimitPhase() == EOpPhase::Intermediate);

    const auto limitInput = limit->GetInput();
    if (CanPushLimitOverInput(limit, limitInput)) {
        // We can push limit over map only.
        const auto map = CastOperator<TOpMap>(limitInput);
        auto limitProps = limit->Props;
        limitProps.StageInputEdges = map->Props.StageInputEdges;
        auto mapProps = map->Props;
        mapProps.StageInputEdges.clear();
        const auto newLimit = MakeIntrusive<TOpLimit>(CastOperator<IUnaryOperator>(limitInput)->GetInput(), limit->Pos, limitProps, limit->GetLimitCond(),
                                                      limit->GetLimitPhase());
        return MakeIntrusive<TOpMap>(newLimit, map->Pos, mapProps, map->GetMapElements());
    } else if (CanPushLimitToStage(limit, limitInput)) {
        auto limitProps = limit->Props;
        limitProps.StageId = limitInput->Props.StageId;
        limitProps.StageInputEdges.clear();
        return MakeIntrusive<TOpLimit>(limitInput, limit->Pos, limitProps, limit->GetLimitCond(), limit->GetLimitPhase());
    } else if (CanPushLimitToRead(limitInput)) {
        const auto read = CastOperator<TOpRead>(limitInput);
        const auto limitCond = limit->GetLimitCond().Node->ChildPtr(1);
        return MakeIntrusive<TOpRead>(read->Alias, read->GetColumns(), read->GetTableStorageType(), read->TableCallable, read->OlapFilterLambda, limitCond,
                                      read->RangeInfo, read->OriginalPredicate, read->SortDir, read->Props, read->Pos);
    }
    return input;
}
} // namespace NKqp
} // namespace NKikimr

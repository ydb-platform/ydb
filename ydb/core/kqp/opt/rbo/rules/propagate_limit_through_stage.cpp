#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

bool IsValidLimit(const TExpression& expression) {
    return expression.Node && !!TMaybeNode<TCoUint64>(expression.Node->ChildPtr(1));
}

bool CanPushLimitToRead(const TIntrusivePtr<TOpLimit>& limit, const TIntrusivePtr<IOperator>& input) {
    if (input->GetKind() != EOperator::Source) {
        return false;
    }
    const auto read = CastOperator<TOpRead>(input);
    return !read->Limit && read->GetTableStorageType() == NYql::EStorageType::ColumnStorage && IsValidLimit(limit->GetLimitCond());
}

bool CanPushLimitOverInput(const TIntrusivePtr<TOpLimit>& limit, const TIntrusivePtr<IOperator>& input) {
    const auto kind = input->GetKind();
    return (kind == EOperator::Map && input->IsSingleConsumer() && limit->Props.StageId == input->Props.StageId);
}

bool CanPushLimitToStage(const TIntrusivePtr<TOpLimit>& limit, const TIntrusivePtr<IOperator>& input) {
    return !(limit->Props.StageId == input->Props.StageId || !input->IsSingleConsumer() ||
             (input->GetKind() == EOperator::Source && CastOperator<TOpRead>(input)->GetTableStorageType() == NYql::EStorageType::RowStorage));
}

bool IsSuitableToPropagateLimitThroughStage(const TIntrusivePtr<IOperator>& input) {
    if (input->GetKind() != EOperator::Limit) {
        return false;
    }
    const auto limit = CastOperator<TOpLimit>(input);
    return limit->GetLimitPhase() != EOpPhase::Final;
}

TIntrusivePtr<TOpLimit> EmitFinalAndIntermediateLimits(const TIntrusivePtr<TOpLimit>& limit) {
    const auto limitCond = limit->GetLimitCond();
    const auto pos = limit->Pos;
    const auto props = limit->Props;
    const auto offset = limit->GetOffsetCond();
    const auto intermediateLimit = MakeIntrusive<TOpLimit>(limit->GetInput(), pos, props, limitCond, EOpPhase::Intermediate);
    return MakeIntrusive<TOpLimit>(intermediateLimit, pos, props, limitCond, offset, EOpPhase::Final);
}

} // namespace

TIntrusivePtr<IOperator> TPropagateLimitThroughStageRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (!IsSuitableToPropagateLimitThroughStage(input)) {
        return input;
    }

    const auto limit = CastOperator<TOpLimit>(input);
    // Split one limit on final and intermediate, we will later propagate intermediate through stages.
    if (limit->GetLimitPhase() == EOpPhase::Undefined) {
        return EmitFinalAndIntermediateLimits(limit);
    }
    Y_ENSURE(limit->GetLimitPhase() == EOpPhase::Intermediate);

    const auto limitInput = limit->GetInput();
    if (CanPushLimitOverInput(limit, limitInput)) {
        // We can push limit over map only.
        const auto map = CastOperator<TOpMap>(limitInput);
        const auto newLimit = MakeIntrusive<TOpLimit>(CastOperator<IUnaryOperator>(limitInput)->GetInput(), limit->Pos, limit->Props, limit->GetLimitCond(),
                                                      limit->GetLimitPhase());
        return MakeIntrusive<TOpMap>(newLimit, map->Pos, map->Props, map->GetMapElements(), map->Project, map->IsOrdered());
    } else if (CanPushLimitToStage(limit, limitInput)) {
        auto props = limit->Props;
        props.StageId = limitInput->Props.StageId;
        return MakeIntrusive<TOpLimit>(limitInput, limit->Pos, props, limit->GetLimitCond(), limit->GetLimitPhase());
    } else if (CanPushLimitToRead(limit, limitInput)) {
        auto read = CastOperator<TOpRead>(limitInput);
        const auto limitCond = limit->GetLimitCond().Node->ChildPtr(1);
        return MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->OutputIUs, read->StorageType, read->TableCallable, read->OlapFilterLambda, limitCond,
                                      read->GetRanges(), read->OriginalPredicate, read->SortDir, read->Props, read->Pos);
    }

    return input;
}
} // namespace NKqp
} // namespace NKikimr

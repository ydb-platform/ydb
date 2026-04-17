#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

bool IsValidLimit(const TExpression& expression) {
    return expression.Node && !!TMaybeNode<TCoUint64>(expression.Node->ChildPtr(1));
}

bool CanPushLimitToSource(const TIntrusivePtr<TOpLimit>& limit, const TIntrusivePtr<IOperator>& input) {
    if (input->GetKind() != EOperator::Source) {
        return false;
    }
    const auto read = CastOperator<TOpRead>(input);
    return !read->Limit && read->GetTableStorageType() == NYql::EStorageType::ColumnStorage && IsValidLimit(limit->GetLimitCond());
}

bool CanPushLimitOverInput(const TIntrusivePtr<IOperator>& input) {
    const auto kind = input->GetKind();
    return ((kind == EOperator::Map) && input->IsSingleConsumer());
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
    auto newOperator = input;

    if (CanPushLimitOverInput(limitInput)) {
        // We can push limit over map only.
        const auto map = CastOperator<TOpMap>(limitInput);
        const auto newLimit = MakeIntrusive<TOpLimit>(CastOperator<IUnaryOperator>(limitInput)->GetInput(), limit->Pos, limit->Props, limit->GetLimitCond(),
                                                      limit->GetLimitPhase());
        newOperator = MakeIntrusive<TOpMap>(newLimit, map->Pos, map->Props, map->GetMapElements(), map->Project, map->IsOrdered());
        newLimit->Props.StageId = newOperator->Props.StageId;
    } else if (CanPushLimitToStage(limit, limitInput)) {
        // Just push limit to stage.
        newOperator->Props.StageId = limitInput->Props.StageId;
        if (CanPushLimitToSource(limit, limitInput)) {
            auto read = CastOperator<TOpRead>(limitInput);
            const auto limitCond = limit->GetLimitCond().Node->ChildPtr(1);
            newOperator = MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->OutputIUs, read->StorageType, read->TableCallable, read->OlapFilterLambda,
                                                 limitCond, read->GetRanges(), read->SortDir, read->Props, read->Pos);
        }
    }

    return newOperator;
}
} // namespace NKqp
} // namespace NKikimr
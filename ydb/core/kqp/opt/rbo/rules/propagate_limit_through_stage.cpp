#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

// As in the old optimizer, an unsorted datashard read takes the limit right over it. The read actor of each task stops
// at the limit, and the read runs one shard at a time, so it stops reading shards once it has the rows.
bool CanPushLimitToRowRead(const TOpLimit& limit, const TOpRead& read, TRBOContext& ctx) {
    // The limit of a datashard read is a literal or a parameter, and a parameter is not pushed yet.
    if (read.SortDir != ESortDir::None || read.Parents.size() != 1 || !TCoUint64::Match(limit.GetLimitCond().GetExpressionBody().Get())) {
        return false;
    }
    const auto& kqpCtx = ctx.KqpCtx;
    // As in the old optimizer, scan queries read row tables without a limit.
    if (kqpCtx.IsScanQuery()) {
        return false;
    }
    // A system view source ignores the limit of the read.
    const auto tablePath = TExprBase(read.GetTable()).Cast<TKqpTable>().Path().StringValue();
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, tablePath);
    return table.Metadata && table.Metadata->Kind != NYql::EKikimrTableKind::SysView;
}

bool CanPushLimitToRead(const TIntrusivePtr<TOpLimit>& limit, const TIntrusivePtr<IOperator>& input, TRBOContext& ctx) {
    if (input->GetKind() != EOperator::Source) {
        return false;
    }
    const auto read = CastOperator<TOpRead>(input);
    if (read->Limit) {
        return false;
    }
    switch (read->GetTableStorageType()) {
        case NYql::EStorageType::ColumnStorage:
            return true;
        case NYql::EStorageType::RowStorage:
            return CanPushLimitToRowRead(*limit, *read, ctx);
        default:
            return false;
    }
}

bool CanPushLimitOverInput(const TIntrusivePtr<TOpLimit>& limit, const TIntrusivePtr<IOperator>& input) {
    const auto kind = input->GetKind();
    return (kind == EOperator::Map && limit->Props.StageId == input->Props.StageId);
}

// The intermediate limit goes to the stage of its input, so that each task of the stage stops at the limit.
bool CanPushLimitToStage(const TIntrusivePtr<TOpLimit>& limit, const TIntrusivePtr<IOperator>& input) {
    return !(limit->Props.StageId == input->Props.StageId || input->Kind == EOperator::Replicate);
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
    } else if (CanPushLimitToRead(limit, limitInput, ctx)) {
        const auto read = CastOperator<TOpRead>(limitInput);
        const auto limitCond = limit->GetLimitCond().Node->ChildPtr(1);
        return MakeIntrusive<TOpRead>(read->Alias, read->GetColumns(), read->GetTableStorageType(), read->TableCallable, read->OlapFilterLambda, limitCond,
                                      read->RangeInfo, read->OriginalPredicate, read->SortDir, read->Props, read->Pos, read->RequireSortedRead);
    }
    return input;
}
} // namespace NKqp
} // namespace NKikimr

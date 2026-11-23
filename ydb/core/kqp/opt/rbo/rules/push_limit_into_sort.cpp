#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool TPushLimitIntoSortRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Limit &&
        input->GetChildren().front()->Kind == EOperator::Sort;
}

TIntrusivePtr<IOperator> TPushLimitIntoSortRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    if (input->Kind != EOperator::Limit) {
        return input;
    }

    auto limit = CastOperator<TOpLimit>(input);
    if (limit->GetInput()->Kind != EOperator::Sort) {
        return input;
    }

    auto sort = CastOperator<TOpSort>(limit->GetInput());
    if (sort->LimitCond) {
        return input;
    }

    sort->LimitCond = limit->GetInputRowsLimit(ctx.ExprCtx, props);
    if (!limit->HasOffset()) {
        return sort;
    }

    // The sort also keeps the rows the offset skips, so the limit stays above it to skip them.
    return MakeIntrusive<TOpLimit>(sort, limit->Pos, limit->Props, limit->LimitCond, limit->GetOffsetCond(), limit->GetLimitPhase());
}
}
}

#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool TPushLimitIntoSortRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Limit &&
        input->Children.front()->Kind == EOperator::Sort;
}

TIntrusivePtr<IOperator> TPushLimitIntoSortRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Limit) {
        return input;
    }

    auto limit = CastOperator<TOpLimit>(input);
    if (limit->Props.EnsureAtMostOne) {
        return input;
    }

    if (limit->HasOffset()) {
        return input;
    }

    if (limit->GetInput()->Kind != EOperator::Sort) {
        return input;
    }

    auto sort = CastOperator<TOpSort>(limit->GetInput());
    if (!sort->IsSingleConsumer() || sort->LimitCond) {
        return input;
    }

    sort->LimitCond = limit->LimitCond;
    return sort;
}
}
}

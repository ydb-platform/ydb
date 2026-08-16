#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool TFuseFiltersRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter &&
        input->Children.front()->Kind == EOperator::Filter;
}

// Match two consequtive filters and fuse them into a single conjunction

TIntrusivePtr<IOperator> TFuseFiltersRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto topFilter = CastOperator<TOpFilter>(input);
    if (topFilter->GetInput()->Kind != Filter) {
        return input;
    }

    auto bottomFilter = CastOperator<TOpFilter>(topFilter->GetInput());

    if (!bottomFilter->IsSingleConsumer()) {
        return input;
    }

    auto conjunctions = topFilter->FilterExpr.SplitConjunct();
    auto bottomConjunctions = bottomFilter->FilterExpr.SplitConjunct();

    conjunctions.insert(conjunctions.end(), bottomConjunctions.begin(), bottomConjunctions.end());

    topFilter->FilterExpr = MakeConjunction(conjunctions);
    topFilter->ReplaceChild(bottomFilter, bottomFilter->GetInput());

    return topFilter;
}
}
}

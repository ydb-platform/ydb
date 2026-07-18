#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool TExtractCommonConjunctsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter;
}

TIntrusivePtr<IOperator> TExtractCommonConjunctsRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    auto newFilterExpr = filter->GetFilterExpression().TryExtractCommonConjuncts();
    if (!newFilterExpr) {
        return input;
    }

    return MakeIntrusive<TOpFilter>(filter->GetInput(), input->Pos, input->Props, *newFilterExpr);
}

}
}

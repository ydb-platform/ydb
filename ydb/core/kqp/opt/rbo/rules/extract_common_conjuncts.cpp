#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

TIntrusivePtr<IOperator> TExtractCommonConjunctsRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    auto newFilterExpr = filter->FilterExpr.TryExtractCommonConjuncts();
    if (!newFilterExpr) {
        return input;
    }

    return MakeIntrusive<TOpFilter>(filter->GetInput(), input->Pos, input->Props, *newFilterExpr);
}

}
}

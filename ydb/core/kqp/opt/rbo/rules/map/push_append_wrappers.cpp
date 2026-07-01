#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

TIntrusivePtr<IOperator> TPushAppendRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    auto result = TPushAppendIntoMapRule().SimpleMatchAndApply(input, ctx, props);
    if (result != input) {
        return result;
    }

    result = TPushAppendThroughUnaryRule(PushUnderFilter).SimpleMatchAndApply(input, ctx, props);
    if (result != input) {
        return result;
    }

    result = TPushAppendThroughAggregateRule().SimpleMatchAndApply(input, ctx, props);
    if (result != input) {
        return result;
    }

    return TPushAppendThroughJoinRule().SimpleMatchAndApply(input, ctx, props);
}

TIntrusivePtr<IOperator> TPushAppendExpressionRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    auto result = TPushAppendIntoMapRule().SimpleMatchAndApply(input, ctx, props);
    if (result != input) {
        return result;
    }

    result = TPushAppendThroughUnaryRule(PushUnderFilter).SimpleMatchAndApply(input, ctx, props);
    if (result != input) {
        return result;
    }

    return TPushAppendThroughJoinRule().SimpleMatchAndApply(input, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr

#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

TIntrusivePtr<IOperator> TPushAppendRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    auto result = TPushMapElementsIntoMapRule().SimpleMatchAndApply(input, ctx, props);
    if (result != input) {
        return result;
    }

    result = TPushMapElementsThroughInputRule(/*pushExpressions*/ false).SimpleMatchAndApply(input, ctx, props);
    if (result != input) {
        return result;
    }

    return TPushMapElementsThroughAggregateRule().SimpleMatchAndApply(input, ctx, props);
}

TIntrusivePtr<IOperator> TPushAppendExpressionRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    auto result = TPushMapElementsIntoMapRule().SimpleMatchAndApply(input, ctx, props);
    if (result != input) {
        return result;
    }

    return TPushMapElementsThroughInputRule(/*pushExpressions*/ true).SimpleMatchAndApply(input, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr

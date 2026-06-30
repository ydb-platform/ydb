#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

bool TPushRenameRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (TPushRenameIntoReadRule().MatchAndApply(input, ctx, props)) {
        return true;
    }
    if (TPushRenameIntoMapProducerRule().MatchAndApply(input, ctx, props)) {
        return true;
    }
    if (TPushRenameIntoAggregateResultRule().MatchAndApply(input, ctx, props)) {
        return true;
    }
    auto output = TPushMapElementsThroughUnaryRule(/*pushExpressions*/ false).SimpleMatchAndApply(input, ctx, props);
    if (output != input) {
        input = output;
        return true;
    }
    output = TPushMapElementsIntoMapRule().SimpleMatchAndApply(input, ctx, props);
    if (output != input) {
        input = output;
        return true;
    }
    output = TPushMapElementsThroughAggregateRule().SimpleMatchAndApply(input, ctx, props);
    if (output != input) {
        input = output;
        return true;
    }
    output = TPushMapElementsThroughJoinRule().SimpleMatchAndApply(input, ctx, props);
    if (output != input) {
        input = output;
        return true;
    }
    return false;
}

} // namespace NKqp
} // namespace NKikimr

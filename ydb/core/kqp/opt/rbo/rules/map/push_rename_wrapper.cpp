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
    if (TPushRenameThroughTransparentUnaryRule(PushAppendAliasesUnderFilter).MatchAndApply(input, ctx, props)) {
        return true;
    }
    if (TPushRenameThroughPassThroughMapRule().MatchAndApply(input, ctx, props)) {
        return true;
    }
    if (TPushRenameThroughAggregateKeyRule().MatchAndApply(input, ctx, props)) {
        return true;
    }
    return TPushRenameThroughJoinSideRule().MatchAndApply(input, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr

#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Given this shape:
// Left Join (L_keys = R_keys)
//     |- L
//     `- R
//
// If R.KeyColumns in R_keys and R is not in LiveOut[LeftJoin]
// then left join can be eliminated, leaving only "L"
TIntrusivePtr<IOperator> TEliminateLeftJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Join) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(input);
    if (join->JoinKind != "Left") {
        return input;
    }

    auto& rhs = join->GetRightInput();

    // R is should not be live
    if (!IUSetIntersect(rhs->GetOutputIUs(), props.LiveOut.find(join.get())->second).empty()) {
        return input;
    }

    // RHS key columns should be covered by RHS join keys.
    if (rhs->Props.Metadata->KeyColumns.empty() || !IUIsSubset(rhs->Props.Metadata->KeyColumns, join->GetRHSKeys())) {
        return input;
    }

    return join->GetLeftInput();
}

} // namespace NKqp
} // namespace NKikimr

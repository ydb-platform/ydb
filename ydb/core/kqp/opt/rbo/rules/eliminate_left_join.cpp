#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

bool TEliminateLeftJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Join;
}

// Given this shape:
// Left Join (L_keys = R_keys)
//     |- L
//     `- R
//
// If R.KeyColumns in R_keys and R is not in LiveOut[LeftJoin]
// then left join can be eliminated, leaving only "L"
TIntrusivePtr<IOperator> TEliminateLeftJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Join) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(input);
    if (join->JoinKind != "Left") {
        return input;
    }

    auto* rhs = join->GetRightInput().Get();

    // No RHS output may be live above the join.
    if (rhs->GetOutputIUs().HasAny(GetLiveOut(join.Get()))) {
        return input;
    }

    // RHS key columns should be covered by RHS join keys.
    const auto& keys = rhs->Props.Metadata->KeyColumns.Unordered();
    if (keys.Empty() || !keys.IsSubsetOf(join->GetRHSKeys())) {
        return input;
    }

    return join->GetLeftInput();
}

} // namespace NKqp
} // namespace NKikimr

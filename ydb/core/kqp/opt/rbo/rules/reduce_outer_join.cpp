#include "kqp_rules_include.h"

namespace NKikimr::NKqp {

bool TReduceOuterJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    if (input->Kind != EOperator::Join) {
        return false;
    }
    const auto& kind = CastOperator<TOpJoin>(input)->JoinKind;
    return kind == "Left" || kind == "Right" || kind == "Full";
}

bool TReduceOuterJoinRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);
    if (!QuickMatch(input)) {
        return false;
    }
    auto& join = *CastOperator<TOpJoin>(input);
    const auto& rejection = join.Props.Analysis.NullRejection;
    Y_ENSURE(rejection, "Outer-join reduction requires null-rejection analysis");
    const auto rejects = [&](size_t nullInput) {
        const auto& nulls = join.GetChild(nullInput)->GetOutputIUs();
        return !nulls.Empty() && rejection->RejectsNulls(nulls);
    };
    // Preserving one input means emitting its unmatched rows with NULLs on
    // the other input. FULL preserves both sides; prove rejection separately.
    const bool preserveLeft = (join.JoinKind == "Left" || join.JoinKind == "Full") && !rejects(1);
    const bool preserveRight = (join.JoinKind == "Right" || join.JoinKind == "Full") && !rejects(0);
    const TString kind = preserveLeft ? (preserveRight ? "Full" : "Left") : (preserveRight ? "Right" : "Inner");
    if (join.JoinKind == kind) {
        return false;
    }
    join.JoinKind = kind;
    // The rule stage invalidates derived properties and repeats, so reducing
    // one join also exposes its ON predicates to simplify lower outer joins.
    return true;
}

} // namespace NKikimr::NKqp

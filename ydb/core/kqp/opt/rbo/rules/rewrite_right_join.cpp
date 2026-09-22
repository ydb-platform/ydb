#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool TRewriteRightJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Join;
}

TIntrusivePtr<IOperator> TRewriteRightJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->GetKind() != EOperator::Join) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(input);
    if (join->JoinKind != "Right" && join->JoinKind != "RightSemi" && join->JoinKind != "RightOnly") {
        return input;
    }

    TString newJoinKind;
    if(join->JoinKind == "Right") {
        newJoinKind = "Left";
    } else if (join->JoinKind == "RightSemi") {
        newJoinKind = "LeftSemi";
    } else /* RightOnly */ {
        newJoinKind = "LeftOnly";
    }

    // Swap join keys
    TVector<TJoinKey> newJoinKeys;
    for (const auto& joinKey : join->JoinKeys) {
        const auto& leftKey = joinKey.Left;
        const auto& rightKey = joinKey.Right;
        newJoinKeys.emplace_back(rightKey, leftKey, joinKey.EqualNulls);
    }

    // Swap arguments
    return MakeIntrusive<TOpJoin>(join->GetRightInput(), join->GetLeftInput(), join->Pos, newJoinKind, newJoinKeys,
                                  join->JoinFilters);
}

}
}

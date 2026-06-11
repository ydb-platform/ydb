#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

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
    TVector<std::pair<TInfoUnit, TInfoUnit>> newJoinKeys;
    for (const auto& [leftKey, rightKey] : join->JoinKeys) {
        newJoinKeys.push_back(std::make_pair(rightKey, leftKey));
    }

    // Swap arguments
    return MakeIntrusive<TOpJoin>(join->GetRightInput(), join->GetLeftInput(), join->Pos, newJoinKind, newJoinKeys);
}

}
}
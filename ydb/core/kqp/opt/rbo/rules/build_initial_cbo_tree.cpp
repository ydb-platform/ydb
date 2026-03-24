#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

/**
 * Initially we build CBO only for joins that don't have other joins or CBO trees as arguments
 * There could be an intermediate filter in between, we also check that
 */
TIntrusivePtr<IOperator> TBuildInitialCBOTreeRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    auto containsJoins = [](const TIntrusivePtr<IOperator>& op) {
        TIntrusivePtr<IOperator> maybeJoin = op;
        if (op->Kind == EOperator::Filter) {
            maybeJoin = CastOperator<TOpFilter>(op)->GetInput();
        }
        return (maybeJoin->Kind == EOperator::Join || maybeJoin->Kind == EOperator::CBOTree);
    };

    if (input->Kind == EOperator::Join) {
        auto join = CastOperator<TOpJoin>(input);
        if (!containsJoins(join->GetLeftInput()) && !containsJoins(join->GetRightInput())) {
            return MakeIntrusive<TOpCBOTree>(input, input->Pos);
        }
    }

    return input;
}
}
}
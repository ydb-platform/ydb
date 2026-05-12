#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {
    
/**
 * Convert unoptimized CBOTrees back into normal operators
 */
TIntrusivePtr<IOperator> TInlineCBOTreeRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind == EOperator::CBOTree) {
        auto cboTree = CastOperator<TOpCBOTree>(input);
        return cboTree->TreeRoot;
    }

    return input;
}
}
}
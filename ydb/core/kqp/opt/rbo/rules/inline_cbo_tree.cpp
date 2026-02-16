#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {
    
/**
 * Convert unoptimized CBOTrees back into normal operators
 */
std::shared_ptr<IOperator> TInlineCBOTreeRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
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
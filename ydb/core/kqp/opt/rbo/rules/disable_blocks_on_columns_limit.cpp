#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

bool IsSuitableToDisableOlapBlocks(const TIntrusivePtr<IOperator>& input, TTypeAnnotationContext& typesCtx, ui32 columnsLimit) {
    if (columnsLimit == 0 || input->GetKind() != EOperator::Limit || typesCtx.BlockEngineMode == NYql::EBlockEngineMode::Disable) {
        return false;
    }
    return true;
}

TIntrusivePtr<IOperator> TDisableBlocksOnColumnsLimitRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    Y_UNUSED(props);
    auto& typesCtx = rboCtx.TypeCtx;
    const ui32 columnsLimit = rboCtx.KqpCtx.Config->GetDisableOlapBlocksOnColumnsLimit();

    if (!IsSuitableToDisableOlapBlocks(input, typesCtx, columnsLimit)) {
        return input;
    }

    if (input->GetOutputIUs().size() >= static_cast<size_t>(columnsLimit)) {
        typesCtx.BlockEngineMode = NYql::EBlockEngineMode::Disable;
    }
    return input;
}

} // namespace NKqp
}

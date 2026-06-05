#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/yql_type_annotation.h>

namespace NKikimr::NKqp {

namespace {

bool IsSuitableToDisableOlapBlocks(const TIntrusivePtr<IOperator>& input, TTypeAnnotationContext& typesCtx, ui32 columnsLimit) {
    if (columnsLimit == 0 || input->GetKind() != EOperator::Limit || typesCtx.BlockEngineMode == NYql::EBlockEngineMode::Disable) {
        return false;
    }
    return true;
}

} // anonymous namespace

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

} // namespace NKikimr::NKqp

#include "type_ann_blocks.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

namespace NYql {
namespace NTypeAnnImpl {

IGraphTransformer::TStatus AsScalarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 1U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureComputable(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto type = input->Head().GetTypeAnn();
    if (type->GetKind() == ETypeAnnotationKind::Block || type->GetKind() == ETypeAnnotationKind::Scalar) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Input type should not be a block or scalar"));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TScalarExprType>(type));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockFuncWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 3U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureBlockOrScalarType(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureBlockOrScalarType(*input->Child(2), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool scalarLeft, scalarRight;
    auto leftItemType = GetBlockItemType(*input->Child(1)->GetTypeAnn(), scalarLeft);
    auto rightItemType = GetBlockItemType(*input->Child(2)->GetTypeAnn(), scalarRight);
    if (scalarLeft && scalarRight) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "At least one input should be a block"));
        return IGraphTransformer::TStatus::Error;
    }

    bool isOptional1;
    const TDataExprType* dataType1;
    if (!EnsureDataOrOptionalOfData(input->Child(1)->Pos(), leftItemType, isOptional1, dataType1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureSpecificDataType(input->Child(1)->Pos(), *dataType1, EDataSlot::Uint64, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool isOptional2;
    const TDataExprType* dataType2;
    if (!EnsureDataOrOptionalOfData(input->Child(2)->Pos(), rightItemType, isOptional2, dataType2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureSpecificDataType(input->Child(2)->Pos(), *dataType2, EDataSlot::Uint64, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TTypeAnnotationNode* retType = dataType1;
    if (isOptional1 || isOptional2) {
        retType = ctx.Expr.MakeType<TOptionalExprType>(retType);
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TBlockExprType>(retType));
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NTypeAnnImpl
}

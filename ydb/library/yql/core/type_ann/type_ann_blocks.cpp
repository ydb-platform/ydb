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

IGraphTransformer::TStatus BlockFuncWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 1U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto name = input->Child(0)->Content();

    for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
        if (!EnsureBlockOrScalarType(*input->Child(i), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!ctx.Types.ArrowResolver) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Arrow resolver isn't available"));
        return IGraphTransformer::TStatus::Error;
    }

    const TTypeAnnotationNode* outType = nullptr;
    TVector<const TTypeAnnotationNode*> argTypes;
    for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
        argTypes.push_back(input->Child(i)->GetTypeAnn());
    }

    if (!ctx.Types.ArrowResolver->LoadFunctionMetadata(ctx.Expr.GetPosition(input->Pos()), name, argTypes, outType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!outType) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "No such Arrow function: " << name));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(outType);
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NTypeAnnImpl
}

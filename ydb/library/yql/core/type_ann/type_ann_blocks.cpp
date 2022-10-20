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

IGraphTransformer::TStatus BlockBitCastWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureBlockOrScalarType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!ctx.Types.ArrowResolver) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Arrow resolver isn't available"));
        return IGraphTransformer::TStatus::Error;
    }

    bool isScalar;
    auto inputType = GetBlockItemType(*input->Child(0)->GetTypeAnn(), isScalar);

    auto outputType = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    bool has = false;
    if (!ctx.Types.ArrowResolver->HasCast(ctx.Expr.GetPosition(input->Pos()), inputType, outputType, has, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!has) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "No such cast"));
        return IGraphTransformer::TStatus::Error;
    }

    if (isScalar) {
        input->SetTypeAnn(ctx.Expr.MakeType<TScalarExprType>(outputType));
    } else {
        input->SetTypeAnn(ctx.Expr.MakeType<TBlockExprType>(outputType));
    }
    
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockCombineAllWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 4U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();
    TTypeAnnotationNode::TListType inputItems;
    for (const auto& type : multiType->GetItems()) {
        if (!EnsureBlockOrScalarType(input->Pos(), *type, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isScalar;
        inputItems.push_back(GetBlockItemType(*type, isScalar));
    }

    if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    ui32 countColumnIndex;
    if (!TryFromString(input->Child(1)->Content(), countColumnIndex) || countColumnIndex >= inputItems.size()) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Bad count column index"));
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureSpecificDataType(input->Child(1)->Pos(), *inputItems[countColumnIndex], EDataSlot::Uint64, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!input->Child(2)->IsCallable("Void")) {
        ui32 filterColumnIndex;
        if (!TryFromString(input->Child(2)->Content(), filterColumnIndex) || filterColumnIndex >= inputItems.size()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Bad filter column index"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Child(2)->Pos(), *inputItems[filterColumnIndex], EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!EnsureTuple(*input->Child(3), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType retMultiType;
    for (const auto& agg : input->Child(3)->Children()) {
        if (!EnsureTupleMinSize(*agg, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!agg->Head().IsCallable("AggBlockApply")) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Expected AggBlockApply"));
            return IGraphTransformer::TStatus::Error;
        }

        if (agg->ChildrenSize() != agg->Head().ChildrenSize()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Different amount of input arguments"));
            return IGraphTransformer::TStatus::Error;
        }

        for (ui32 i = 1; i < agg->ChildrenSize(); ++i) {
            ui32 argColumnIndex;
            if (!TryFromString(agg->Child(i)->Content(), argColumnIndex) || argColumnIndex >= inputItems.size()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Bad arg column index"));
                return IGraphTransformer::TStatus::Error;
            }

            auto applyArgType = agg->Head().Child(i)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (!IsSameAnnotation(*inputItems[argColumnIndex], *applyArgType)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() <<
                    "Mismatch argument type, expected: " << *applyArgType << ", got: " << *inputItems[argColumnIndex]));
                return IGraphTransformer::TStatus::Error;
            }
        }

        retMultiType.push_back(AggApplySerializedStateType(agg->HeadPtr(), ctx.Expr));
    }

    auto outputItemType = ctx.Expr.MakeType<TMultiExprType>(retMultiType);
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NTypeAnnImpl
}

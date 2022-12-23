#include "type_ann_blocks.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>


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

IGraphTransformer::TStatus BlockCompressWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType blockItemTypes;
    if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (blockItemTypes.size() < 2) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Expected at least two columns, got " << blockItemTypes.size()));
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    ui32 index = 0;
    if (!TryFromString(input->Child(1)->Content(), index)) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()),
                          TStringBuilder() << "Failed to convert to integer: " << input->Child(1)->Content()));
        return IGraphTransformer::TStatus::Error;
    }

    if (index >= blockItemTypes.size() - 1) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()),
                          TStringBuilder() << "Index out of range. Index: " << index << ", maximum is: " << blockItemTypes.size() - 1));
        return IGraphTransformer::TStatus::Error;
    }

    auto bitmapType = blockItemTypes[index];
    if (bitmapType->GetKind() != ETypeAnnotationKind::Data || bitmapType->Cast<TDataExprType>()->GetSlot() != NUdf::EDataSlot::Bool) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()),
                          TStringBuilder() << "Expecting Bool as bitmap column type, but got: " << *bitmapType));
        return IGraphTransformer::TStatus::Error;
    }

    auto flowItemTypes = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
    flowItemTypes.erase(flowItemTypes.begin() + index);

    auto outputItemType = ctx.Expr.MakeType<TMultiExprType>(flowItemTypes);
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockExpandChunkedWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 1U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType blockItemTypes;
    const bool allowChunked = true;
    if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr, allowChunked)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto flowItemTypes = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
    bool hasChunked = false;
    for (auto& flowItemType : flowItemTypes) {
        if (flowItemType->GetKind() == ETypeAnnotationKind::ChunkedBlock) {
            hasChunked = true;
            flowItemType = ctx.Expr.MakeType<TBlockExprType>(flowItemType->Cast<TChunkedBlockExprType>()->GetItemType());
        }
    }

    if (!hasChunked) {
        output = input->HeadPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    auto outputItemType = ctx.Expr.MakeType<TMultiExprType>(flowItemTypes);
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockCoalesceWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto first  = input->Child(0);
    auto second = input->Child(1);
    if (!EnsureBlockOrScalarType(*first, ctx.Expr) ||
        !EnsureBlockOrScalarType(*second, ctx.Expr))
    {
        return IGraphTransformer::TStatus::Error;
    }

    bool firstIsScalar;
    auto firstItemType = GetBlockItemType(*first->GetTypeAnn(), firstIsScalar);
    bool firstIsOptional = firstItemType->GetKind() == ETypeAnnotationKind::Optional;
    firstItemType = RemoveOptionalType(firstItemType);

    bool secondIsScalar;
    auto secondItemType = GetBlockItemType(*second->GetTypeAnn(), secondIsScalar);
    bool secondIsOptional = secondItemType->GetKind() == ETypeAnnotationKind::Optional;
    secondItemType = RemoveOptionalType(secondItemType);

    if (!IsSameAnnotation(*firstItemType, *secondItemType)) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() <<
            "Mismatch item types: first is " << *firstItemType << ", second is " << *secondItemType));
        return IGraphTransformer::TStatus::Error;
    }

    if (!firstIsOptional) {
        output = input->HeadPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    auto outputItemType = secondItemType;
    if (secondIsOptional) {
        outputItemType = ctx.Expr.MakeType<TOptionalExprType>(outputItemType);
    }
    if (firstIsScalar && secondIsScalar) {
        input->SetTypeAnn(ctx.Expr.MakeType<TScalarExprType>(outputItemType));
    } else {
        input->SetTypeAnn(ctx.Expr.MakeType<TBlockExprType>(outputItemType));
    }
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockLogicalWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    const ui32 args = input->IsCallable("BlockNot") ? 1 : 2;
    if (!EnsureArgsCount(*input, args, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool isOptionalResult = false;
    bool allScalars = true;
    for (ui32 i = 0U; i < input->ChildrenSize() ; ++i) {
        auto child = input->Child(i);
        if (!EnsureBlockOrScalarType(*child, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        bool isScalar;
        const TTypeAnnotationNode* blockItemType = GetBlockItemType(*child->GetTypeAnn(), isScalar);

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(input->Child(i)->Pos(), blockItemType, isOptional, dataType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Child(i)->Pos(), *dataType, EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        isOptionalResult = isOptionalResult || isOptional;
        allScalars = allScalars && isScalar;
    }

    const TTypeAnnotationNode* resultType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool);
    if (isOptionalResult) {
        resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
    }

    if (allScalars) {
        resultType = ctx.Expr.MakeType<TScalarExprType>(resultType);
    } else {
        resultType = ctx.Expr.MakeType<TBlockExprType>(resultType);
    }
    input->SetTypeAnn(resultType);
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

bool ValidateBlockKeys(TPositionHandle pos, const TTypeAnnotationNode::TListType& inputItems,
    const TExprNode& keys, TTypeAnnotationNode::TListType& retMultiType, TExprContext& ctx) {
    if (!EnsureTupleMinSize(keys, 1, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (auto child : keys.Children()) {
        if (!EnsureAtom(*child, ctx)) {
            return false;
        }

        ui32 keyColumnIndex;
        if (!TryFromString(child->Content(), keyColumnIndex) || keyColumnIndex >= inputItems.size()) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Bad key column index"));
            return false;
        }

        retMultiType.push_back(inputItems[keyColumnIndex]);
    }

    return true;
}

bool ValidateBlockAggs(TPositionHandle pos, const TTypeAnnotationNode::TListType& inputItems, const TExprNode& aggs,
    TTypeAnnotationNode::TListType& retMultiType, TExprContext& ctx, bool overState, bool many) {
    if (!EnsureTuple(aggs, ctx)) {
        return false;
    }

    for (const auto& agg : aggs.Children()) {
        if (!EnsureTupleMinSize(*agg, 1, ctx)) {
            return false;
        }

        auto expectedCallable = overState ? "AggBlockApplyState" : "AggBlockApply";
        if (!agg->Head().IsCallable(expectedCallable)) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Expected: " << expectedCallable));
            return false;
        }

        if (agg->ChildrenSize() != agg->Head().ChildrenSize()) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Different amount of input arguments"));
            return false;
        }

        for (ui32 i = 1; i < agg->ChildrenSize(); ++i) {
            ui32 argColumnIndex;
            if (!TryFromString(agg->Child(i)->Content(), argColumnIndex) || argColumnIndex >= inputItems.size()) {
                ctx.AddError(TIssue(ctx.GetPosition(pos), "Bad arg column index"));
                return false;
            }

            if (many && inputItems[argColumnIndex]->GetKind() != ETypeAnnotationKind::Optional) {
                ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() <<
                    "Expected optional state, but got: " << *inputItems[argColumnIndex]));
                return false;
            }

            auto applyArgType = agg->Head().Child(i)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            auto expectedType = many ? ctx.MakeType<TOptionalExprType>(applyArgType) : applyArgType;
            if (!IsSameAnnotation(*inputItems[argColumnIndex], *expectedType)) {
                ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() <<
                    "Mismatch argument type, expected: " << *expectedType << ", got: " << *inputItems[argColumnIndex]));
                return false;
            }
        }

        auto retAggType = overState ? agg->HeadPtr()->GetTypeAnn() : AggApplySerializedStateType(agg->HeadPtr(), ctx);
        retMultiType.push_back(retAggType);
    }

    return true;
}

IGraphTransformer::TStatus BlockCombineAllWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 3U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType blockItemTypes;
    if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!input->Child(1)->IsCallable("Void")) {
        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        ui32 filterColumnIndex;
        if (!TryFromString(input->Child(1)->Content(), filterColumnIndex) || filterColumnIndex >= blockItemTypes.size()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Bad filter column index"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Child(1)->Pos(), *blockItemTypes[filterColumnIndex], EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    TTypeAnnotationNode::TListType retMultiType;
    if (!ValidateBlockAggs(input->Pos(), blockItemTypes, *input->Child(2), retMultiType, ctx.Expr, false, false)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto outputItemType = ctx.Expr.MakeType<TMultiExprType>(retMultiType);
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockCombineHashedWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 4U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType blockItemTypes;
    if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!input->Child(1)->IsCallable("Void")) {
        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        ui32 filterColumnIndex;
        if (!TryFromString(input->Child(1)->Content(), filterColumnIndex) || filterColumnIndex >= blockItemTypes.size()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Bad filter column index"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Child(1)->Pos(), *blockItemTypes[filterColumnIndex], EDataSlot::Bool, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!EnsureTupleMinSize(*input->Child(2), 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType retMultiType;
    if (!ValidateBlockKeys(input->Pos(), blockItemTypes, *input->Child(2), retMultiType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!ValidateBlockAggs(input->Pos(), blockItemTypes, *input->Child(3), retMultiType, ctx.Expr, false, false)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (auto& t : retMultiType) {
        t = ctx.Expr.MakeType<TBlockExprType>(t);
    }

    retMultiType.push_back(ctx.Expr.MakeType<TScalarExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64)));
    auto outputItemType = ctx.Expr.MakeType<TMultiExprType>(retMultiType);
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockMergeFinalizeHashedWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    const bool many = input->Content().EndsWith("ManyFinalizeHashed");
    if (!EnsureArgsCount(*input, 3U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType blockItemTypes;
    if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType retMultiType;
    if (!ValidateBlockKeys(input->Pos(), blockItemTypes, *input->Child(1), retMultiType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!ValidateBlockAggs(input->Pos(), blockItemTypes, *input->Child(2), retMultiType, ctx.Expr, true, many)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (auto& t : retMultiType) {
        t = ctx.Expr.MakeType<TBlockExprType>(t);
    }

    retMultiType.push_back(ctx.Expr.MakeType<TScalarExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64)));
    auto outputItemType = ctx.Expr.MakeType<TMultiExprType>(retMultiType);
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NTypeAnnImpl
}

#include "type_ann_blocks.h"
#include "type_ann_impl.h"
#include "type_ann_list.h"
#include "type_ann_wide.h"
#include "type_ann_pg.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>

namespace NYql {
namespace NTypeAnnImpl {

namespace {

const TTypeAnnotationNode* MakeBlockOrScalarType(const TTypeAnnotationNode* blockItemType, bool isScalar, TExprContext& ctx) {
    if (isScalar) {
        return ctx.MakeType<TScalarExprType>(blockItemType);
    } else {
        return ctx.MakeType<TBlockExprType>(blockItemType);
    }
}

} // namespace

IGraphTransformer::TStatus AsScalarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
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

    if (!EnsureSupportedAsBlockType(input->Pos(), *type, ctx.Expr, ctx.Types)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TScalarExprType>(type));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus ReplicateScalarWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto source = input->Child(0);
    if (!EnsureScalarType(*source, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto length = input->Child(1);
    if (!EnsureScalarType(*length, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TTypeAnnotationNode* lengthItemType = length->GetTypeAnn()->Cast<TScalarExprType>()->GetItemType();
    if (!EnsureSpecificDataType(length->Pos(), *lengthItemType, EDataSlot::Uint64, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TBlockExprType>(source->GetTypeAnn()->Cast<TScalarExprType>()->GetItemType()));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus ReplicateScalarsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureMinArgsCount(*input, 1, ctx.Expr) || !EnsureMaxArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType blockItemTypes;
    if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto flowItemTypes = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
    YQL_ENSURE(flowItemTypes.size() > 0);

    TMaybe<THashSet<ui32>> replicateIndexes;
    if (input->ChildrenSize() == 2) {
        if (!EnsureTupleOfAtoms(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        replicateIndexes.ConstructInPlace();
        for (auto& atom : input->Child(1)->ChildrenList()) {
            ui32 idx;
            if (!TryFromString(atom->Content(), idx)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(atom->Pos()),
                    TStringBuilder() << "Expecting integer as replicate index, got: " << atom->Content()));
                return IGraphTransformer::TStatus::Error;
            }
            if (idx >= flowItemTypes.size() - 1) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(atom->Pos()),
                    TStringBuilder() << "Replicate index too big: " << idx << ", should be less than " << (flowItemTypes.size() - 1)));
                return IGraphTransformer::TStatus::Error;
            }
            if (!replicateIndexes->insert(idx).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(atom->Pos()), TStringBuilder() << "Duplicate replicate index " << idx));
                return IGraphTransformer::TStatus::Error;
            }
            if (flowItemTypes[idx]->GetKind() != ETypeAnnotationKind::Scalar) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(atom->Pos()), TStringBuilder() << "Invalid replicate index " << idx << ": input item is not scalar"));
                return IGraphTransformer::TStatus::Error;
            }
        }
    }

    bool hasScalarsToConvert = false;
    size_t inputScalarsCount = 0;
    for (size_t i = 0; i + 1 < flowItemTypes.size(); ++i) {
        auto& itemType = flowItemTypes[i];
        if (itemType->IsScalar()) {
            ++inputScalarsCount;
            if (!replicateIndexes.Defined() || replicateIndexes->contains(i)) {
                hasScalarsToConvert = true;
                itemType = ctx.Expr.MakeType<TBlockExprType>(itemType->Cast<TScalarExprType>()->GetItemType());
            }
        }
    }

    if (!hasScalarsToConvert) {
        output = input->HeadPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    if (replicateIndexes.Defined() && replicateIndexes->size() == inputScalarsCount) {
        auto children = input->ChildrenList();
        children.resize(1);
        output = ctx.Expr.ChangeChildren(*input, std::move(children));
        return IGraphTransformer::TStatus::Repeat;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(ctx.Expr.MakeType<TMultiExprType>(flowItemTypes)));
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

IGraphTransformer::TStatus BlockExistsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureBlockOrScalarType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool isScalar;
    const TTypeAnnotationNode* blockItemType = GetBlockItemType(*input->Head().GetTypeAnn(), isScalar);

    // At this point BlockItem type should be either an Optional or a Pg one.
    // All other cases should be handled in the previous transform phases.
    if (blockItemType->GetKind() != ETypeAnnotationKind::Optional &&
        blockItemType->GetKind() != ETypeAnnotationKind::Pg)
    {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() <<
            "Expecting Optional or Pg type as an argument, but got: " << *blockItemType));
        return IGraphTransformer::TStatus::Error;
    }

    const TTypeAnnotationNode* resultType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Bool);
    input->SetTypeAnn(MakeBlockOrScalarType(resultType, isScalar, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockExpandChunkedWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 1U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType itemTypes;
    TTypeAnnotationNode::TListType blockItemTypes;

    if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream) {
        if (!EnsureWideStreamBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        itemTypes = input->Head().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
    } else {
        if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        itemTypes = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
    }
    
    bool allScalars = AllOf(itemTypes, [](const TTypeAnnotationNode* item) { return item->GetKind() == ETypeAnnotationKind::Scalar; });
    if (allScalars) {
        output = input->HeadPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    input->SetTypeAnn(input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockCoalesceWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
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
    if (firstItemType->GetKind() != ETypeAnnotationKind::Optional && firstItemType->GetKind() != ETypeAnnotationKind::Pg) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(first->Pos()), TStringBuilder() <<
            "Expecting Optional or Pg type as first argument, but got: " << *firstItemType));
        return IGraphTransformer::TStatus::Error;
    }

    bool secondIsScalar;
    auto secondItemType = GetBlockItemType(*second->GetTypeAnn(), secondIsScalar);

    if (!IsSameAnnotation(*firstItemType, *secondItemType) &&
        !IsSameAnnotation(*RemoveOptionalType(firstItemType), *secondItemType))
    {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() <<
            "Uncompatible coalesce types: first is " << *firstItemType << ", second is " << *secondItemType));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(MakeBlockOrScalarType(secondItemType, firstIsScalar && secondIsScalar, ctx.Expr));
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

    input->SetTypeAnn(MakeBlockOrScalarType(resultType, allScalars, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockIfWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 3U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto pred = input->Child(0);
    auto thenNode  = input->Child(1);
    auto elseNode = input->Child(2);

    if (!EnsureBlockOrScalarType(*pred, ctx.Expr) ||
        !EnsureBlockOrScalarType(*thenNode, ctx.Expr) ||
        !EnsureBlockOrScalarType(*elseNode, ctx.Expr))
    {
        return IGraphTransformer::TStatus::Error;
    }

    bool predIsScalar;
    const TTypeAnnotationNode* predItemType = GetBlockItemType(*pred->GetTypeAnn(), predIsScalar);
    if (!EnsureSpecificDataType(pred->Pos(), *predItemType, NUdf::EDataSlot::Bool, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool thenIsScalar;
    const TTypeAnnotationNode* thenItemType = GetBlockItemType(*thenNode->GetTypeAnn(), thenIsScalar);

    bool elseIsScalar;
    const TTypeAnnotationNode* elseItemType = GetBlockItemType(*elseNode->GetTypeAnn(), elseIsScalar);

    if (!IsSameAnnotation(*thenItemType, *elseItemType)) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() <<
            "Mismatch item types: then branch is " << *thenItemType << ", else branch is " << *elseItemType));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(MakeBlockOrScalarType(thenItemType, predIsScalar && thenIsScalar && elseIsScalar, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockJustWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto child = input->Child(0);
    if (!EnsureBlockOrScalarType(*child, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    bool isScalar;
    const TTypeAnnotationNode* blockItemType = GetBlockItemType(*child->GetTypeAnn(), isScalar);
    const TTypeAnnotationNode* resultType = ctx.Expr.MakeType<TOptionalExprType>(blockItemType);

    input->SetTypeAnn(MakeBlockOrScalarType(resultType, isScalar, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockAsStructWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TVector<const TItemExprType*> members;
    bool onlyScalars = true;
    for (auto& child : input->Children()) {
        auto nameNode = child->Child(0);
        if (!EnsureAtom(*nameNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto valueNode = child->Child(1);
        if (!EnsureBlockOrScalarType(*valueNode, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isScalar;
        const TTypeAnnotationNode* blockItemType = GetBlockItemType(*valueNode->GetTypeAnn(), isScalar);

        onlyScalars = onlyScalars && isScalar;
        members.push_back(ctx.Expr.MakeType<TItemExprType>(nameNode->Content(), blockItemType));
    }

    auto structType = ctx.Expr.MakeType<TStructExprType>(members);
    if (!structType->Validate(input->Pos(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto less = [](const TExprNode::TPtr& left, const TExprNode::TPtr& right) {
        return left->Head().Content() < right->Head().Content();
    };

    if (!IsSorted(input->Children().begin(), input->Children().end(), less)) {
        auto list = input->ChildrenList();
        Sort(list.begin(), list.end(), less);
        output = ctx.Expr.ChangeChildren(*input, std::move(list));
        return IGraphTransformer::TStatus::Repeat;
    }

    const TTypeAnnotationNode* resultType;
    if (onlyScalars) {
        resultType = ctx.Expr.MakeType<TScalarExprType>(structType);
    } else {
        resultType = ctx.Expr.MakeType<TBlockExprType>(structType);
    }
    input->SetTypeAnn(resultType);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockAsTupleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType items;
    bool onlyScalars = true;
    for (const auto& child : input->Children()) {
        if (!EnsureBlockOrScalarType(*child, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isScalar;
        const TTypeAnnotationNode* blockItemType = GetBlockItemType(*child->GetTypeAnn(), isScalar);
        onlyScalars = onlyScalars && isScalar;
        items.push_back(blockItemType);
    }

    const TTypeAnnotationNode* resultType = ctx.Expr.MakeType<TTupleExprType>(items);
    input->SetTypeAnn(MakeBlockOrScalarType(resultType, onlyScalars, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockMemberWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto& child = input->Head();
    if (!EnsureBlockOrScalarType(child, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool isScalar;
    const TTypeAnnotationNode* blockItemType = GetBlockItemType(*child.GetTypeAnn(), isScalar);
    const TTypeAnnotationNode* resultType;
    if (IsNull(*blockItemType)) {
        resultType = blockItemType;
    } else {
        const TStructExprType* structType;
        bool isOptional;
        if (blockItemType->GetKind() == ETypeAnnotationKind::Optional) {
            auto itemType = blockItemType->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureStructType(child.Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            structType = itemType->Cast<TStructExprType>();
            isOptional = true;
        } else {
            if (!EnsureStructType(child.Pos(), *blockItemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            structType = blockItemType->Cast<TStructExprType>();
            isOptional = false;
        }

        if (!EnsureComputableType(input->Head().Pos(), *structType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto memberName = input->Tail().Content();
        auto pos = FindOrReportMissingMember(memberName, input->Pos(), *structType, ctx.Expr);
        if (!pos) {
            return IGraphTransformer::TStatus::Error;
        }

        resultType = structType->GetItems()[*pos]->GetItemType();
        if (isOptional && !resultType->IsOptionalOrNull()) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }
    }

    input->SetTypeAnn(MakeBlockOrScalarType(resultType, isScalar, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}


IGraphTransformer::TStatus BlockNthWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto child = input->Child(0);
    if (!EnsureBlockOrScalarType(*child, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool isScalar;
    const TTypeAnnotationNode* blockItemType = GetBlockItemType(*child->GetTypeAnn(), isScalar);
    const TTypeAnnotationNode* resultType;
    if (IsNull(*blockItemType)) {
        resultType = blockItemType;
    } else {
        const TTupleExprType* tupleType;
        bool isOptional;
        if (blockItemType->GetKind() == ETypeAnnotationKind::Optional) {
            auto itemType = blockItemType->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureTupleType(child->Pos(), *itemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            tupleType = itemType->Cast<TTupleExprType>();
            isOptional = true;
        }
        else {
            if (!EnsureTupleType(child->Pos(), *blockItemType, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            tupleType = blockItemType->Cast<TTupleExprType>();
            isOptional = false;
        }

        if (!EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        ui32 index = 0;
        if (!TryFromString(input->Tail().Content(), index)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Failed to convert to integer: " << input->Tail().Content()));
            return IGraphTransformer::TStatus::Error;
        }

        if (index >= tupleType->GetSize()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Index out of range. Index: " <<
                index << ", size: " << tupleType->GetSize()));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComputableType(input->Head().Pos(), *tupleType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        resultType = tupleType->GetItems()[index];
        if (isOptional && !resultType->IsOptionalOrNull()) {
            resultType = ctx.Expr.MakeType<TOptionalExprType>(resultType);
        }
    }

    input->SetTypeAnn(MakeBlockOrScalarType(resultType, isScalar, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockToPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto child = input->Child(0);
    if (!EnsureBlockOrScalarType(*child, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    bool isScalar;
    const TTypeAnnotationNode* blockItemType = GetBlockItemType(*child->GetTypeAnn(), isScalar);
    auto resultType = ToPgImpl(input->Pos(), blockItemType, ctx.Expr);
    if (!resultType) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(MakeBlockOrScalarType(resultType, isScalar, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockFromPgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto child = input->Child(0);
    if (!EnsureBlockOrScalarType(*child, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    bool isScalar;
    const TTypeAnnotationNode* blockItemType = GetBlockItemType(*child->GetTypeAnn(), isScalar);
    auto resultType = FromPgImpl(input->Pos(), blockItemType, ctx.Expr);
    if (!resultType) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(MakeBlockOrScalarType(resultType, isScalar, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockFuncWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto name = input->Child(0)->Content();
    Y_UNUSED(name);
    if (auto status = EnsureTypeRewrite(input->ChildRef(1), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    auto returnType = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    if (!EnsureBlockOrScalarType(input->Child(1)->Pos(), *returnType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (ui32 i = 2; i < input->ChildrenSize(); ++i) {
        if (!EnsureBlockOrScalarType(*input->Child(i), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    // TODO: more validation
    input->SetTypeAnn(returnType);
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

    auto castStatus = ctx.Types.ArrowResolver->HasCast(ctx.Expr.GetPosition(input->Pos()), inputType, outputType, ctx.Expr);
    if (castStatus == IArrowResolver::ERROR) {
        return IGraphTransformer::TStatus::Error;
    } else if (castStatus == IArrowResolver::NOT_FOUND) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "No such cast"));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(MakeBlockOrScalarType(outputType, isScalar, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

bool ValidateBlockKeys(TPositionHandle pos, const TTypeAnnotationNode::TListType& inputItems,
    TExprNode& keys, TTypeAnnotationNode::TListType& retMultiType, TExprContext& ctx) {
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

bool ValidateBlockAggs(TPositionHandle pos, const TTypeAnnotationNode::TListType& inputItems, TExprNode& aggs,
    TTypeAnnotationNode::TListType& retMultiType, TExprContext& ctx, bool overState, bool many) {
    if (!EnsureTuple(aggs, ctx)) {
        return false;
    }

    for (const auto& agg : aggs.Children()) {
        if (!EnsureTupleMinSize(*agg, 1, ctx)) {
            return false;
        }

        if (overState) {
            if (!EnsureTupleSize(*agg, 2, ctx)) {
                return false;
            }
        }

        auto expectedCallable = overState ? "AggBlockApplyState" : "AggBlockApply";
        if (!agg->Head().IsCallable(expectedCallable)) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Expected: " << expectedCallable));
            return false;
        }

        if (agg->ChildrenSize() + (overState ? 1 : 0) != agg->Head().ChildrenSize()) {
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

            auto applyArgType = agg->Head().Child(i + (overState ? 1 : 0))->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
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
    if (!EnsureArgsCount(*input, many ? 5U : 3U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType blockItemTypes;
    if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    YQL_ENSURE(blockItemTypes.size() > 0);

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

    if (many) {
        if (!EnsureAtom(*input->Child(3), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        ui32 streamIndex;
        if (!TryFromString(input->Child(3)->Content(), streamIndex) || streamIndex >= blockItemTypes.size() - 1) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(3)->Pos()), "Bad stream index"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSpecificDataType(input->Child(3)->Pos(), *blockItemTypes[streamIndex], EDataSlot::Uint32, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!ValidateAggManyStreams(*input->Child(4), input->Child(2)->ChildrenSize(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        // disallow any scalar columns except for streamIndex column
        auto itemTypes = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
        for (ui32 i = 0; i + 1 < itemTypes.size(); ++i) {
            bool isScalar = itemTypes[i]->GetKind() == ETypeAnnotationKind::Scalar;
            if (isScalar && i != streamIndex) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Unexpected scalar type " << *itemTypes[i] << ", on input column #" << i));
                return IGraphTransformer::TStatus::Error;
            }
        }
    }

    retMultiType.push_back(ctx.Expr.MakeType<TScalarExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64)));
    auto outputItemType = ctx.Expr.MakeType<TMultiExprType>(retMultiType);
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideToBlocksWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 1U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();
    TTypeAnnotationNode::TListType retMultiType;
    for (const auto& type : multiType->GetItems()) {
        if (type->IsBlockOrScalar()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Input type should not be a block or scalar"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureSupportedAsBlockType(input->Pos(), *type, ctx.Expr, ctx.Types)) {
            return IGraphTransformer::TStatus::Error;
        }

        retMultiType.push_back(ctx.Expr.MakeType<TBlockExprType>(type));
    }

    retMultiType.push_back(ctx.Expr.MakeType<TScalarExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64)));
    auto outputItemType = ctx.Expr.MakeType<TMultiExprType>(retMultiType);
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideFromBlocksWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 1U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType retMultiType;
    if (!EnsureWideFlowBlockType(input->Head(), retMultiType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    YQL_ENSURE(!retMultiType.empty());
    retMultiType.pop_back();
    auto outputItemType = ctx.Expr.MakeType<TMultiExprType>(retMultiType);
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideSkipTakeBlocksWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType blockItemTypes;
    if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    output = input;
    const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
    auto convertStatus = TryConvertTo(input->ChildRef(1), *expectedType, ctx.Expr);
    if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Can not convert argument to Uint64"));
        return IGraphTransformer::TStatus::Error;
    }

    if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
        return convertStatus;
    }

    input->SetTypeAnn(input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideTopBlocksWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 3U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType blockItemTypes;
    if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    output = input;
    const TTypeAnnotationNode* expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
    auto convertStatus = TryConvertTo(input->ChildRef(1), *expectedType, ctx.Expr);
    if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), "Can not convert argument to Uint64"));
        return IGraphTransformer::TStatus::Error;
    }

    if (convertStatus.Level != IGraphTransformer::TStatus::Ok) {
        return convertStatus;
    }

    if (!ValidateWideTopKeys(input->Tail(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideSortBlocksWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType blockItemTypes;
    if (!EnsureWideFlowBlockType(input->Head(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!ValidateWideTopKeys(input->Tail(), blockItemTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockPgOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*input, 4, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto name = input->Head().Content();
    if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TVector<ui32> argTypes;
    bool allScalars = true;
    for (ui32 i = 2; i < input->ChildrenSize(); ++i) {
        if (!EnsureBlockOrScalarType(*input->Child(i), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isScalar;
        auto itemType = GetBlockItemType(*input->Child(i)->GetTypeAnn(), isScalar);
        if (itemType->GetKind() != ETypeAnnotationKind::Pg) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected PG type, but got: " << *itemType));
            return IGraphTransformer::TStatus::Error;
        }

        allScalars = allScalars && isScalar;
        ui32 argType = itemType->Cast<TPgExprType>()->GetId();
        argTypes.push_back(argType);
    }

    auto operId = FromString<ui32>(input->Child(1)->Content());
    const auto& oper = NPg::LookupOper(operId, argTypes);
    if (oper.Name != name) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Mismatch of resolved operator name, expected: " << name << ", but got:" << oper.Name));
        return IGraphTransformer::TStatus::Error;
    }

    auto result = ctx.Expr.MakeType<TPgExprType>(oper.ResultType);
    input->SetTypeAnn(MakeBlockOrScalarType(result, allScalars, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockPgCallWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto name = input->Head().Content();

    if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureTuple(*input->Child(2), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (const auto& setting : input->Child(2)->Children()) {
        if (!EnsureTupleMinSize(*setting, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(setting->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto content = setting->Head().Content();
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Unexpected setting " << content << " in function " << name));

        return IGraphTransformer::TStatus::Error;
    }

    TVector<ui32> argTypes;
    bool allScalars = true;
    for (ui32 i = 3; i < input->ChildrenSize(); ++i) {
        if (!EnsureBlockOrScalarType(*input->Child(i), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isScalar;
        auto itemType = GetBlockItemType(*input->Child(i)->GetTypeAnn(), isScalar);
        if (itemType->GetKind() != ETypeAnnotationKind::Pg) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Expected PG type, but got: " << *itemType));
            return IGraphTransformer::TStatus::Error;
        }

        allScalars = allScalars && isScalar;
        ui32 argType = itemType->Cast<TPgExprType>()->GetId();
        argTypes.push_back(argType);
    }

    auto procId = FromString<ui32>(input->Child(1)->Content());
    const auto& proc = NPg::LookupProc(procId, argTypes);
    if (proc.Name != name) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Mismatch of resolved function name, expected: " << name << ", but got:" << proc.Name));
        return IGraphTransformer::TStatus::Error;
    }

    if (proc.Kind == NPg::EProcKind::Window) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Window function " << name << " cannot be called directly"));
        return IGraphTransformer::TStatus::Error;
    }

    if (proc.Kind == NPg::EProcKind::Aggregate) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Aggregate function " << name << " cannot be called directly"));
        return IGraphTransformer::TStatus::Error;
    }

    const TTypeAnnotationNode* result = ctx.Expr.MakeType<TPgExprType>(proc.ResultType);
    if (proc.ReturnSet) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
            TStringBuilder() << "Not supported return set"));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(MakeBlockOrScalarType(result, allScalars, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus BlockExtendWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TTypeAnnotationNode::TListType commonItemTypes;
    for (size_t idx = 0; idx < input->ChildrenSize(); ++idx) {
        auto child = input->Child(idx);        
        TTypeAnnotationNode::TListType currentItemTypes;
        if (!EnsureWideFlowBlockType(*child, idx ? currentItemTypes : commonItemTypes, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (idx == 0) {
            continue;
        }

        if (currentItemTypes.size() != commonItemTypes.size()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()),
                TStringBuilder() << "Expected same width ( " << commonItemTypes.size() << ") on all inputs, but got: " << *child->GetTypeAnn() << " on input #" << idx));
            return IGraphTransformer::TStatus::Error;
        }


        for (size_t i = 0; i < currentItemTypes.size(); ++i) {
            if (!IsSameAnnotation(*currentItemTypes[i], *commonItemTypes[i])) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()),
                    TStringBuilder() << "Expected item type " << *commonItemTypes[i] << " at column #" << i << " on input #" << idx << ", but got : " << *currentItemTypes[i]));
                return IGraphTransformer::TStatus::Error;
            }
        }
    }

    TTypeAnnotationNode::TListType resultItemTypes;
    for (size_t i = 0; i < commonItemTypes.size(); ++i) {
        bool isScalar = (i + 1 == commonItemTypes.size());
        resultItemTypes.emplace_back(MakeBlockOrScalarType(commonItemTypes[i], isScalar, ctx.Expr));
    }
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(ctx.Expr.MakeType<TMultiExprType>(std::move(resultItemTypes))));
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NTypeAnnImpl
}

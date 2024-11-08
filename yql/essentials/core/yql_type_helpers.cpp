#include "yql_type_helpers.h"

#include <util/string/builder.h>


namespace NYql {

TSet<TStringBuf> GetColumnsOfStructOrSequenceOfStruct(const TTypeAnnotationNode& type) {
    const TTypeAnnotationNode* itemType = nullptr;
    if (type.GetKind() != ETypeAnnotationKind::Struct) {
        itemType = GetSeqItemType(&type);
        YQL_ENSURE(itemType);
    } else {
        itemType = &type;
    }

    TSet<TStringBuf> result;
    for (auto& item : itemType->Cast<TStructExprType>()->GetItems()) {
        result.insert(item->GetName());
    }

    return result;
}

namespace {

bool SilentGetSequenceItemType(TPosition pos, const TTypeAnnotationNode& inputType, bool allowMultiIO,
                               const TTypeAnnotationNode*& result, TIssue& error)
{
    result = nullptr;
    const auto itemType = GetSeqItemType(&inputType);
    if (!itemType) {
        error = TIssue(pos, TStringBuilder() << "Expected list, stream, flow or optional, but got: " << inputType);
        return false;
    }

    if (allowMultiIO && itemType->GetKind() != ETypeAnnotationKind::Struct && itemType->GetKind() != ETypeAnnotationKind::Multi) {
        if (itemType->GetKind() != ETypeAnnotationKind::Variant) {
            error = TIssue(pos, TStringBuilder() << "Expected Struct or Variant as row type, but got: " << *itemType);
            return false;
        }
        auto varType = itemType->Cast<TVariantExprType>()->GetUnderlyingType();
        TTypeAnnotationNode::TListType varItemTypes;
        if (varType->GetKind() == ETypeAnnotationKind::Struct) {
            for (auto item: varType->Cast<TStructExprType>()->GetItems()) {
                varItemTypes.push_back(item->GetItemType());
            }
        } else {
            varItemTypes = varType->Cast<TTupleExprType>()->GetItems();
        }
        if (varItemTypes.size() < 2) {
            error = TIssue(pos, TStringBuilder() << "Expected at least two items in Variant row type, but got: " << varItemTypes.size());
            return false;
        }
        for (auto varItemType: varItemTypes) {
            if (varItemType->GetKind() != ETypeAnnotationKind::Struct) {
                error = TIssue(pos, TStringBuilder() << "Expected Struct in Variant item type, but got: " << *varItemType);
                return false;
            }
        }
    } else {
        if (itemType->GetKind() != ETypeAnnotationKind::Struct && itemType->GetKind() != ETypeAnnotationKind::Multi) {
            error = TIssue(pos, TStringBuilder() << "Expected Struct or Multi as row type, but got: " << *itemType);
            return false;
        }
    }

    result = itemType;
    YQL_ENSURE(result);
    return true;
}

}

const TTypeAnnotationNode* GetSequenceItemType(NNodes::TExprBase listNode, bool allowMultiIO) {
    const TTypeAnnotationNode* itemType = nullptr;
    TIssue error;
    if (!SilentGetSequenceItemType({}, *listNode.Ref().GetTypeAnn(), allowMultiIO, itemType, error)) {
        // position is not used
        YQL_ENSURE(false, "" << error.GetMessage());
    }
    return itemType;
}

const TTypeAnnotationNode* GetSequenceItemType(NNodes::TExprBase listNode, bool allowMultiIO, TExprContext& ctx) {
    return GetSequenceItemType(listNode.Pos(), listNode.Ref().GetTypeAnn(), allowMultiIO, ctx);
}

const TTypeAnnotationNode* GetSequenceItemType(TPositionHandle pos, const TTypeAnnotationNode* inputType, bool allowMultiIO,
                                               TExprContext& ctx)
{
    const TTypeAnnotationNode* itemType = nullptr;
    TIssue error;
    if (!SilentGetSequenceItemType(ctx.GetPosition(pos), *inputType, allowMultiIO, itemType, error)) {
        ctx.AddError(error);
        return nullptr;
    }
    return itemType;
}

bool GetSequenceItemType(const TExprNode& list, const TTypeAnnotationNode*& itemType, TExprContext& ctx) {
    itemType = GetSequenceItemType(list.Pos(), list.GetTypeAnn(), false, ctx);
    return itemType != nullptr;
}

const TTypeAnnotationNode* SilentGetSequenceItemType(const TExprNode& list, bool allowMultiIO) {
    TIssue error;
    const TTypeAnnotationNode* itemType = nullptr;
    if (!SilentGetSequenceItemType({}, *list.GetTypeAnn(), allowMultiIO, itemType, error)) {
        return nullptr;
    }
    return itemType;
}

} // namespace NYql

#include "qlfilter.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <utility>

namespace NYql {

namespace {

TMaybe<EDataSlot> GetQLCompatibleDataSlot(const TExprNode::TPtr& node, bool allowOptional) {
    bool isOptional = false;
    const TDataExprType* dataType = nullptr;
    if (!IsDataOrOptionalOfData(node->GetTypeAnn(), isOptional, dataType)) {
        return {};
    }
    if (!allowOptional && isOptional) {
        return {};
    }
    if (!dataType) {
        return {};
    }
    const auto dataSlot = dataType->GetSlot();
    if (IsDataTypeNumeric(dataSlot) || dataSlot == EDataSlot::Bool || dataSlot == EDataSlot::String || dataSlot == EDataSlot::Utf8) {
        return {dataSlot};
    }
    return {};
}

bool NodeHasQLCompatibleType(const TExprNode::TPtr& node, bool allowOptional) {
    return GetQLCompatibleDataSlot(node, allowOptional).Defined();
}

bool ComparisonHasQLCompatibleType(const TExprNode::TPtr& left, const TExprNode::TPtr& right, bool allowOptional) {
    const auto dataSlotLeft = GetQLCompatibleDataSlot(left, allowOptional);
    if (!dataSlotLeft) {
        return false;
    }
    const auto dataSlotRight = GetQLCompatibleDataSlot(right, allowOptional);
    if (!dataSlotRight) {
        return false;
    }
    return *dataSlotLeft == *dataSlotRight
        || IsDataTypeIntegral(*dataSlotLeft) && IsDataTypeIntegral(*dataSlotRight)
        || IsDataTypeFloat(*dataSlotLeft) && IsDataTypeFloat(*dataSlotRight)
        || IsDataTypeString(*dataSlotLeft) && IsDataTypeString(*dataSlotRight);
}

TExprNode::TPtr ConvertQLConst(const TExprNode::TPtr& node, const TExprNode::TPtr& rowArg, bool allowOptional) {
    if (!NodeHasQLCompatibleType(node, allowOptional)) {
        return nullptr;
    }
    if (IsDepended(*node, *rowArg)) {
        return nullptr;
    }
    return node;
}

TExprNode::TPtr ConvertQLMember(const TExprNode::TPtr& node, const TExprNode::TPtr& rowArg, const TExprNode::TPtr& newRowArg, TExprContext& ctx, bool allowOptional) {
    if (!node->IsCallable("Member")) {
        return nullptr;
    }
    YQL_ENSURE(node->ChildrenSize() == 2);
    if (node->ChildPtr(0) != rowArg) {
        return nullptr;
    }
    const auto memberName = node->Child(1)->Content();
    if (memberName.StartsWith("_yql_sys_")) {
        return nullptr;
    }
    if (!NodeHasQLCompatibleType(node, allowOptional)) {
        return nullptr;
    }
    auto arg = newRowArg;
    return ctx.ChangeChild(*node, 0, std::move(arg));
}

TExprNode::TPtr ConvertQLComparison(const TExprNode::TPtr& node, const TExprNode::TPtr& rowArg, const TExprNode::TPtr& newRowArg, TExprContext& ctx, bool allowOptional) {
    YQL_ENSURE(node->ChildrenSize() == 2);
    if (!ComparisonHasQLCompatibleType(node->ChildPtr(0), node->ChildPtr(1), allowOptional)) {
        return nullptr;
    }
    TExprNode::TPtr childLeft;
    TExprNode::TPtr childRight;
    if (childLeft = ConvertQLMember(node->ChildPtr(0), rowArg, newRowArg, ctx, allowOptional)) {
        childRight = ConvertQLConst(node->ChildPtr(1), rowArg, allowOptional);
    }
    else if (childRight = ConvertQLMember(node->ChildPtr(1), rowArg, newRowArg, ctx, allowOptional)) {
        childLeft = ConvertQLConst(node->ChildPtr(0), rowArg, allowOptional);
    }
    if (!childLeft || !childRight) {
        return nullptr;
    }
    return ctx.ChangeChildren(*node, {childLeft, childRight});
}

TExprNode::TPtr ConvertQLSubTree(const TExprNode::TPtr& node, const TExprNode::TPtr& rowArg, const TExprNode::TPtr& newRowArg, TExprContext& ctx, bool allowOptional = false) {
    if (node->IsCallable("Exists")) {
        auto converted = ConvertQLMember(node->HeadPtr(), rowArg, newRowArg, ctx, /*allowOptional*/ true);
        return converted ? ctx.ChangeChild(*node, 0, std::move(converted)) : nullptr;
    }
    if (node->IsCallable({"And", "Or", "Not"})) {
        TExprNode::TListType convertedChildren;
        for (const auto& child : node->ChildrenList()) {
            const auto converted = ConvertQLSubTree(child, rowArg, newRowArg, ctx, allowOptional);
            if (!converted) {
                return nullptr;
            }
            convertedChildren.push_back(converted);
        };
        return ctx.ChangeChildren(*node, std::move(convertedChildren));
    }
    if (node->IsCallable("Coalesce")) {
        if (node->ChildrenSize() != 2) {
            return nullptr;
        }
        const auto comparison = node->Child(0);
        if (!comparison->IsCallable({"<", "<=", ">", ">=", "==", "!="})) {
            return nullptr;
        }
        const auto nullValue = node->Child(1);
        if (!nullValue->IsCallable("Bool")) {
            return nullptr;
        }
        const auto convertedComparison = ConvertQLComparison(comparison, rowArg, newRowArg, ctx, /*allowOptional*/ true);
        if (!convertedComparison) {
            return nullptr;
        }
        return ctx.ChangeChildren(*node, {convertedComparison, nullValue});
    }
    if (node->IsCallable({"<", "<=", ">", ">=", "==", "!="})) {
        return ConvertQLComparison(node, rowArg, newRowArg, ctx, allowOptional);
    }
    if (node->IsCallable("Bool")) {
        return node;
    }
    if (node->IsCallable("Member")) {
        return ConvertQLMember(node, rowArg, newRowArg, ctx, allowOptional);
    }
    return nullptr;
}

} // namespace

TExprNode::TPtr ExtractQLFilterPredicate(
    const TExprNode::TPtr& predicate,
    const TExprNode::TPtr& rowArg,
    const TExprNode::TPtr& newRowArg,
    TPositionHandle pos,
    TExprContext& ctx)
{
    if (!predicate->IsCallable("And")) {
        return ConvertQLSubTree(predicate, rowArg, newRowArg, ctx);
    }

    TExprNode::TListType compatibleParts;
    for (const auto& child : predicate->ChildrenList()) {
        if (auto converted = ConvertQLSubTree(child, rowArg, newRowArg, ctx)) {
            compatibleParts.push_back(std::move(converted));
        }
    }

    if (compatibleParts.empty()) {
        return nullptr;
    }
    if (compatibleParts.size() == 1) {
        return std::move(compatibleParts.front());
    }
    return ctx.NewCallable(pos, "And", std::move(compatibleParts));
}

} // namespace NYql

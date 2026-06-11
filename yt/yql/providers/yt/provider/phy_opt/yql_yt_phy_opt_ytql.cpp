#include "yql_yt_phy_opt.h"

#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>

#include <yql/essentials/core/dq_expr_nodes/dq_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NYql {

using namespace NNodes;
using namespace NNodes::NDq;
using namespace NPrivate;

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
    if (node->IsCallable({"And", "Or", "Not", "Exists"})) {
        TExprNode::TListType convertedChildren;
        // TODO: Allow optional inside Exists.
        // const bool allowOptionalForChildren = allowOptional || node->IsCallable("Exists");
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

} // empty namespace

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ExtractQLFilters(TExprBase node, TExprContext& ctx) const {
    const auto opMap = node.Cast<TYtMap>();
    if (opMap.Input().Size() > 1) {
        return node;
    }

    if (NYql::HasAnySetting(opMap.Settings().Ref(), EYtSettingType::WeakFields)) {
        return node;
    }

    const auto section = opMap.Input().Item(0).Cast<TYtSection>();
    if (NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::UserSchema | EYtSettingType::UserColumns)) {
        return node;
    }

    for (const auto& path : section.Paths()) {
        const TYtPath ytPath(path);
        if (!ytPath.QLFilter().Maybe<TCoVoid>()) {
            return node;
        }
        const auto tableInfo = TYtTableBaseInfo::Parse(ytPath.Table());
        if (!tableInfo->RowSpec || !tableInfo->RowSpec->StrictSchema) {
            return node;
        }
        if (tableInfo->Meta && tableInfo->Meta->Attrs.contains("schema_mode") && tableInfo->Meta->Attrs["schema_mode"] == "weak") {
            return node;
        }
        if (NYql::HasAnySetting(tableInfo->Settings.Ref(), EYtSettingType::UserSchema | EYtSettingType::UserColumns)) {
            return node;
        }
    }

    const auto flatMap =  GetFlatMapOverInputStream(opMap.Mapper());
    if (!flatMap) {
        return node;
    }

    const auto flatMapLambda = flatMap.Lambda();
    if (!flatMapLambda) {
        return node;
    }

    const auto optionalIf = flatMapLambda.Body().Maybe<TCoOptionalIf>();
    if (!optionalIf) {
        return node;
    }

    const auto rowArg = flatMapLambda.Cast().Args().Arg(0).Ptr();
    const auto newRowArg = ctx.NewArgument(rowArg->Pos(), rowArg->Content());

    TExprNode::TListType qlCompatibleParts;
    TExprNode::TListType otherParts;
    TExprNode::TPtr predicate = optionalIf.Cast().Predicate().Ptr();
    auto convertOne = [&] (const TExprNode::TPtr& node) {
        if (const auto converted = ConvertQLSubTree(node, rowArg, newRowArg, ctx)) {
            qlCompatibleParts.push_back(converted);
        } else {
            otherParts.push_back(node);
        }
    };

    if (!predicate->IsCallable("And")) {
        convertOne(predicate);
    } else {
        for (const auto& child : predicate->ChildrenList()) {
            convertOne(child);
        };
    }

    if (qlCompatibleParts.empty()) {
        return node;
    }

    TExprNode::TPtr qlCompatiblePredicate;
    if (qlCompatibleParts.size() == 1) {
        qlCompatiblePredicate = qlCompatibleParts.front();
    } else {
        qlCompatiblePredicate = ctx.NewCallable(flatMap.Cast().Pos(), "And", std::move(qlCompatibleParts));
    }
    YQL_ENSURE(qlCompatiblePredicate);

    const auto typeNode = ExpandType(rowArg->Pos(), *rowArg->GetTypeAnn(), ctx);
    const auto lambdaNode = ctx.NewLambda(qlCompatiblePredicate->Pos(), ctx.NewArguments(qlCompatiblePredicate->Pos(), {newRowArg}), std::move(qlCompatiblePredicate));
    const auto qlFilter = ctx.NewCallable(flatMap.Cast().Pos(), "YtQLFilter", {typeNode, lambdaNode});

    auto newPaths = section.Paths().Ref().ChildrenList();
    for (auto& path : newPaths) {
        path = Build<TYtPath>(ctx, path->Pos())
            .InitFrom(TYtPath(path))
            .QLFilter(qlFilter)
            .Done().Ptr();
    }
    auto newSection = ctx.ChangeChild(section.Ref(), TYtSection::idx_Paths, ctx.NewList(section.Paths().Pos(), std::move(newPaths)));
    auto newSectionList = Build<TYtSectionList>(ctx, opMap.Input().Pos())
        .Add(newSection)
        .Done().Ptr();

    return ctx.ChangeChild(opMap.Ref(), TYtMap::idx_Input, std::move(newSectionList));
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::OptimizeQLFilterType(TExprBase node, TExprContext& ctx) const {
    const auto qlFilter = node.Cast<TYtQLFilter>();
    const auto rowType = qlFilter.RowType();

    const auto arg = qlFilter.Predicate().Args().Arg(0).Ptr();
    TSet<TStringBuf> memberSet;
    VisitExpr(qlFilter.Predicate().Body().Ptr(), [&arg, &memberSet](const TExprNode::TPtr& n) {
        if (n->IsCallable("Member")) {
            if (n->ChildPtr(0) == arg) {
                const auto member = n->Child(1);
                YQL_ENSURE(member->IsAtom());
                memberSet.insert(member->Content());
            }
            return false;
        }
        return true;
    });

    TExprNode::TListType newRowTypeChildren;
    for (const auto& child : rowType.Ptr()->ChildrenList()) {
        const auto name = child->Child(0);
        if (memberSet.contains(name->Content())) {
            newRowTypeChildren.push_back(child);
        }
    }
    if (newRowTypeChildren.size() == rowType.Ptr()->ChildrenSize()) {
        return node;
    }

    auto newRowType = ctx.ChangeChildren(rowType.Ref(), std::move(newRowTypeChildren));
    return ctx.ChangeChild(qlFilter.Ref(), TYtQLFilter::idx_RowType, std::move(newRowType));
}

}  // namespace NYql

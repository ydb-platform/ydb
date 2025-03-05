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

bool NodeHasQLCompatibleType(const TExprNode::TPtr& node) {
    bool isOptional = false;
    const TDataExprType* dataType = nullptr;
    if (!IsDataOrOptionalOfData(node->GetTypeAnn(), isOptional, dataType)) {
        return false;
    }
    if (isOptional) {
        return false;
    }
    if (!dataType) {
        return false;
    }
    if (!IsDataTypeIntegral(dataType->GetSlot())) {
        return false;
    }
    return true;
}

TExprNode::TPtr CheckQLConst(const TExprNode::TPtr& node, const TExprNode::TPtr& rowArg) {
    if (!NodeHasQLCompatibleType(node)) {
        return nullptr;
    }
    if (IsDepended(*node, *rowArg)) {
        return nullptr;
    }
    return node;
}

TExprNode::TPtr ConvertQLMember(const TExprNode::TPtr& node, const TExprNode::TPtr& rowArg, const TExprNode::TPtr& newRowArg, TExprContext& ctx) {
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
    if (!NodeHasQLCompatibleType(node)) {
        return nullptr;
    }
    auto arg = newRowArg;
    return ctx.ChangeChild(*node, 0, std::move(arg));
}

TExprNode::TPtr ConvertQLComparison(const TExprNode::TPtr& node, const TExprNode::TPtr& rowArg, const TExprNode::TPtr& newRowArg, TExprContext& ctx) {
    YQL_ENSURE(node->ChildrenSize() == 2);
    TExprNode::TPtr childLeft;
    TExprNode::TPtr childRight;
    if (childLeft = ConvertQLMember(node->ChildPtr(0), rowArg, newRowArg, ctx)) {
        childRight = CheckQLConst(node->ChildPtr(1), rowArg);
    }
    else if (childRight = ConvertQLMember(node->ChildPtr(1), rowArg, newRowArg, ctx)) {
        childLeft = CheckQLConst(node->ChildPtr(0), rowArg);
    }
    if (!childLeft || !childRight) {
        return nullptr;
    }
    return ctx.ChangeChildren(*node, {childLeft, childRight});
}

TExprNode::TPtr ConvertQLSubTree(const TExprNode::TPtr& node, const TExprNode::TPtr& rowArg, const TExprNode::TPtr& newRowArg, TExprContext& ctx) {
    if (node->IsCallable({"And", "Or", "Not"})) {
        TExprNode::TListType convertedChildren;
        for (const auto& child : node->ChildrenList()) {
            const auto converted = ConvertQLSubTree(child, rowArg, newRowArg, ctx);
            if (!converted) {
                return nullptr;
            }
            convertedChildren.push_back(converted);
        };
        return ctx.ChangeChildren(*node, std::move(convertedChildren));
    }
    if (node->IsCallable({"<", "<=", ">", ">=", "==", "!="})) {
        return ConvertQLComparison(node, rowArg, newRowArg, ctx);
    }
    return nullptr;
}

} // empty namespace

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::ExtractQLFilters(TExprBase node, TExprContext& ctx) const {
    const auto opMap = node.Cast<TYtMap>();
    if (opMap.Input().Size() > 1) {
        return node;
    }

    const auto section = opMap.Input().Item(0);
    if (NYql::HasAnySetting(opMap.Settings().Ref(), EYtSettingType::QLFilter | EYtSettingType::WeakFields)) {
        return node;
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

    if (!predicate->IsCallable("And")) {
        const auto converted = ConvertQLSubTree(predicate, rowArg, newRowArg, ctx);
        if (converted) {
            qlCompatibleParts.push_back(converted);
        } else {
            otherParts.push_back(predicate);
        }
    } else {
        for (const auto& child : predicate->ChildrenList()) {
            const auto converted = ConvertQLSubTree(child, rowArg, newRowArg, ctx);
            if (converted) {
                qlCompatibleParts.push_back(converted);
            } else {
                otherParts.push_back(child);
            }
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

    TExprNode::TPtr prunedPredicate;
    if (otherParts.empty()) {
        prunedPredicate = MakeBool<true>(predicate->Pos(), ctx);
    } else if (otherParts.size() == 1) {
        prunedPredicate = otherParts.front();
    } else {
        prunedPredicate = ctx.NewCallable(predicate->Pos(), "And", std::move(otherParts));
    }
    YQL_ENSURE(prunedPredicate);

    const auto typeNode = ExpandType(rowArg->Pos(), *rowArg->GetTypeAnn(), ctx);
    const auto lambdaNode = ctx.NewLambda(qlCompatiblePredicate->Pos(), ctx.NewArguments(qlCompatiblePredicate->Pos(), {newRowArg}), std::move(qlCompatiblePredicate));
    const auto qlFilter = ctx.NewCallable(flatMap.Cast().Pos(), "YtQLFilter", {typeNode, lambdaNode});

    auto newOpMap = ctx.ChangeChild(opMap.Ref(), TYtMap::idx_Settings, NYql::AddSetting(opMap.Settings().Ref(), EYtSettingType::QLFilter, qlFilter, ctx));
    const bool pruneLambda = State_->Configuration->PruneQLFilterLambda.Get().GetOrElse(DEFAULT_PRUNE_QL_FILTER_LAMBDA);
    if (pruneLambda) {
        const auto newFlatMap = Build<TCoFlatMapBase>(ctx, flatMap.Cast().Pos())
            .InitFrom(flatMap.Cast())
            .Lambda()
                .InitFrom(flatMapLambda.Cast())
                .Body<TCoOptionalIf>()
                    .InitFrom(optionalIf.Cast())
                    .Predicate(prunedPredicate)
                .Build()
            .Build()
        .Done().Ptr();

        const TOptimizeExprSettings settings{State_->Types};
        const TNodeOnNodeOwnedMap remaps{{flatMap.Cast().Raw(), newFlatMap}};
        const auto status = RemapExpr(newOpMap, newOpMap, remaps, ctx, settings);
        YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error);
    }
    return newOpMap;
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

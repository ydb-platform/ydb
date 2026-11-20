#include "yql_yt_phy_opt.h"

#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>

#include <yt/yql/providers/yt/lib/qlfilter/qlfilter.h>

#include <yql/essentials/core/dq_expr_nodes/dq_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NYql {

using namespace NNodes;
using namespace NNodes::NDq;
using namespace NPrivate;

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

    auto qlCompatiblePredicate = ExtractQLFilterPredicate(
        optionalIf.Cast().Predicate().Ptr(),
        rowArg,
        newRowArg,
        flatMap.Cast().Pos(),
        ctx);
    if (!qlCompatiblePredicate) {
        return node;
    }

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

} // namespace NYql

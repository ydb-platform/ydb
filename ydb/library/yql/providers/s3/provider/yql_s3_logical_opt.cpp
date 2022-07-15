#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>


namespace NYql {

using namespace NNodes;

namespace {

void RebuildPredicateForPruning(const TExprNode::TPtr& pred, const TExprNode& arg, const TStructExprType& extraType,
                                TExprNode::TPtr& prunedPred, TExprNode::TPtr& extraPred, TExprContext& ctx)
{
    if (pred->IsCallable({"And", "Or"})) {
        TExprNodeList childPruned;
        TExprNodeList childExtra;
        for (auto& child : pred->ChildrenList()) {
            childPruned.emplace_back();
            TExprNode::TPtr extra;
            RebuildPredicateForPruning(child, arg, extraType, childPruned.back(), extra, ctx);
            if (extra) {
                childExtra.emplace_back(std::move(extra));
            }
        }
        YQL_ENSURE(pred->ChildrenSize() > 0);
        if (pred->IsCallable("Or") && childExtra.size() < pred->ChildrenSize() || childExtra.empty()) {
            prunedPred = pred;
            extraPred = nullptr;
            return;
        }

        prunedPred = ctx.ChangeChildren(*pred, std::move(childPruned));
        extraPred = ctx.ChangeChildren(*pred, std::move(childExtra));
        return;
    }

    // analyze remaining predicate part
    bool usedNonExtraMembers = false;
    VisitExpr(*pred, [&](const TExprNode& node) {
        if (node.IsCallable("Member") && &node.Head() == &arg) {
            auto col = node.Tail().Content();
            if (!extraType.FindItem(col)) {
                usedNonExtraMembers = true;
            }
            return false;
        }

        if (&node == &arg) {
            usedNonExtraMembers = false;
            return false;
        }

        return !usedNonExtraMembers;
    });

    if (usedNonExtraMembers) {
        prunedPred = pred;
        extraPred = nullptr;
    } else {
        prunedPred = MakeBool(pred->Pos(), true, ctx);
        extraPred = pred;
    }
}

TCoFlatMapBase CalculatePrunedPaths(TCoFlatMapBase flatMap, TExprContext& ctx, TTypeAnnotationContext* types) {
    auto dqSource = flatMap.Input().Cast<TDqSourceWrap>();
    auto extraColumns = GetSetting(dqSource.Settings().Ref(), "extraColumns");
    if (!extraColumns) {
        return flatMap;
    }

    YQL_ENSURE(extraColumns->ChildrenSize() == 2);
    const TStructExprType* extraType = extraColumns->Tail().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    TExprNode::TPtr pred = flatMap.Lambda().Body().Cast<TCoConditionalValueBase>().Predicate().Ptr();

    TOptimizeExprSettings optimizeExprSettings(types);
    optimizeExprSettings.VisitLambdas = false;
    optimizeExprSettings.VisitChanges = true;
    OptimizeExpr(pred, pred, [&](const TExprNode::TPtr& node, TExprContext& ctx) {
        if (node->IsCallable("Not")) {
            if (node->Head().IsCallable({"And", "Or"})) {
                auto children = node->Head().ChildrenList();
                for (auto& child : children) {
                    child = ctx.NewCallable(child->Pos(), "Not", { child });
                }
                return ctx.NewCallable(node->Head().Pos(), node->Head().IsCallable("Or") ? "And" : "Or", std::move(children));
            }
            if (node->Head().IsCallable("Not")) {
                return node->Head().HeadPtr();
            }
        }
        return node;
    }, ctx, optimizeExprSettings);

    const TExprNode& arg = flatMap.Lambda().Args().Arg(0).Ref();
    TExprNode::TPtr prunedPred;
    TExprNode::TPtr extraPred;

    RebuildPredicateForPruning(pred, arg, *extraType, prunedPred, extraPred, ctx);
    YQL_ENSURE(prunedPred);
    TExprNode::TPtr filteredPathList;
    if (extraPred) {
        auto source = flatMap.Input().Cast<TDqSourceWrap>().Input().Cast<TS3SourceSettingsBase>();
        TExprNodeList pathList;
        for (size_t i = 0; i < source.Paths().Size(); ++i) {
            pathList.push_back(
                ctx.Builder(source.Paths().Item(i).Pos())
                    .List()
                        .Callable(0, "String")
                            .Add(0, source.Paths().Item(i).Path().Ptr())
                        .Seal()
                        .Callable(1, "Uint64")
                            .Add(0, source.Paths().Item(i).Size().Ptr())
                        .Seal()
                        .Add(2, source.Paths().Item(i).ExtraColumns().Ptr())
                    .Seal()
                .Build()
            );
        }

        filteredPathList = ctx.Builder(pred->Pos())
            .Callable("EvaluateExpr")
                .Callable(0, "OrderedFilter")
                    .Add(0, ctx.NewCallable(pred->Pos(), "AsList", std::move(pathList)))
                    .Lambda(1)
                        .Param("item")
                        .Apply(ctx.NewLambda(extraPred->Pos(), flatMap.Lambda().Args().Ptr(), std::move(extraPred)))
                            .With(0)
                                .Callable("Nth")
                                    .Arg(0, "item")
                                    .Atom(1, "2", TNodeFlags::Default)
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    auto newLambda = flatMap.Lambda();
    if (filteredPathList) {
        auto cvBase = flatMap.Lambda().Body().Cast<TCoConditionalValueBase>();
        newLambda = Build<TCoLambda>(ctx, flatMap.Lambda().Pos())
            .Args(flatMap.Lambda().Args())
            .Body<TCoConditionalValueBase>()
                .CallableName(cvBase.CallableName())
                .Predicate(prunedPred)
                .Value(cvBase.Value())
            .Build()
            .Done();
    }

    return Build<TCoFlatMapBase>(ctx, flatMap.Pos())
        .CallableName(flatMap.CallableName())
        .Input<TDqSourceWrap>()
            .InitFrom(dqSource)
            .Settings(AddSetting(dqSource.Settings().Ref(), dqSource.Settings().Cast().Pos(), "prunedPaths", filteredPathList, ctx))
        .Build()
        .Lambda(newLambda)
        .Done();
}

class TS3LogicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TS3LogicalOptProposalTransformer(TS3State::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderS3, {})
        , State_(state)
    {
#define HNDL(name) "LogicalOptimizer-"#name, Hndl(&TS3LogicalOptProposalTransformer::name)
        AddHandler(0, &TCoLeft::Match, HNDL(TrimReadWorld));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(TryPrunePaths));
        AddHandler(0, &TDqSourceWrap::Match, HNDL(ApplyPrunedPath));
#undef HNDL
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        const auto& maybeRead = node.Cast<TCoLeft>().Input().Maybe<TS3ReadObject>();
        if (!maybeRead) {
            return node;
        }

        return TExprBase(ctx.NewWorld(node.Pos()));
    }

    TMaybeNode<TExprBase> ApplyPrunedPath(TExprBase node, TExprContext& ctx) const {
        const TDqSourceWrap dqSource = node.Cast<TDqSourceWrap>();
        if (dqSource.DataSource().Category() != S3ProviderName) {
            return node;
        }

        const auto& maybeS3SourceSettings = dqSource.Input().Maybe<TS3SourceSettingsBase>();
        if (!maybeS3SourceSettings || dqSource.Ref().ChildrenSize() <= TDqSourceWrap::idx_Settings) {
            return node;
        }

        auto prunedPathSetting = GetSetting(dqSource.Settings().Ref(), "prunedPaths");
        if (!prunedPathSetting || prunedPathSetting->ChildrenSize() == 1) {
            return node;
        }

        auto extraColumnsSetting = GetSetting(dqSource.Settings().Ref(), "extraColumns");
        YQL_ENSURE(extraColumnsSetting);
        auto extraColumns = extraColumnsSetting->ChildPtr(1);
        YQL_ENSURE(extraColumns->IsCallable("AsList"), "extraColumns should have literal value");

        auto prunedPaths = prunedPathSetting->ChildPtr(1);
        if (prunedPaths->IsCallable("List")) {
            YQL_CLOG(INFO, ProviderS3) << "S3 Paths completely pruned: " << extraColumns->ChildrenSize() << " paths";
            return ctx.NewCallable(node.Pos(), "List", { ExpandType(node.Pos(), *node.Ref().GetTypeAnn(), ctx) });
        }

        YQL_ENSURE(prunedPaths->IsCallable("AsList"), "prunedPaths should have literal value");
        YQL_ENSURE(prunedPaths->ChildrenSize() <= extraColumns->ChildrenSize());
        YQL_ENSURE(prunedPaths->ChildrenSize() > 0);

        auto newSettings = ReplaceSetting(dqSource.Settings().Ref(), prunedPathSetting->Pos(), "prunedPaths", nullptr, ctx);
        if (prunedPaths->ChildrenSize() == extraColumns->ChildrenSize()) {
            YQL_CLOG(INFO, ProviderS3) << "No S3 paths are pruned: " << extraColumns->ChildrenSize() << " paths";
            return Build<TDqSourceWrap>(ctx, dqSource.Pos())
                .InitFrom(dqSource)
                .Settings(newSettings)
                .Done();
        }

        YQL_CLOG(INFO, ProviderS3) << "Pruning S3 Paths: " << extraColumns->ChildrenSize() << " -> " << prunedPaths->ChildrenSize();
        TExprNodeList newPaths;
        TExprNodeList newExtraColumns;

        for (auto& entry : prunedPaths->ChildrenList()) {
            auto path = entry->ChildPtr(0);
            auto size = entry->ChildPtr(1);
            auto extra = entry->ChildPtr(2);

            YQL_ENSURE(path->IsCallable("String"));
            YQL_ENSURE(size->IsCallable("Uint64"));
            YQL_ENSURE(extra->IsCallable("AsStruct"));

            newPaths.push_back(ctx.NewList(entry->Pos(), { path->HeadPtr(), size->HeadPtr(), extra }));
            newExtraColumns.push_back(extra);
        }

        newSettings = ReplaceSetting(*newSettings, newSettings->Pos(), "extraColumns", ctx.NewCallable(newSettings->Pos(), "AsList", std::move(newExtraColumns)), ctx);
        auto oldSrc = dqSource.Input().Cast<TS3SourceSettingsBase>();
        auto newSrc = ctx.ChangeChild(dqSource.Input().Ref(), TS3SourceSettingsBase::idx_Paths, ctx.NewList(oldSrc.Paths().Pos(), std::move(newPaths)));

        return Build<TDqSourceWrap>(ctx, dqSource.Pos())
            .InitFrom(dqSource)
            .Input(newSrc)
            .Settings(newSettings)
            .Done();
    }

    TMaybeNode<TExprBase> TryPrunePaths(TExprBase node, TExprContext& ctx) const {
        const TCoFlatMapBase flatMap = node.Cast<TCoFlatMapBase>();
        if (!flatMap.Lambda().Body().Maybe<TCoConditionalValueBase>()) {
            return node;
        }

        const auto& maybeDqSource = flatMap.Input().Maybe<TDqSourceWrap>();
        if (!maybeDqSource) {
            return node;
        }

        TDqSourceWrap dqSource = maybeDqSource.Cast();
        if (dqSource.DataSource().Category() != S3ProviderName) {
            return node;
        }

        const auto& maybeS3SourceSettings = dqSource.Input().Maybe<TS3SourceSettingsBase>();
        if (!maybeS3SourceSettings || dqSource.Ref().ChildrenSize() <= TDqSourceWrap::idx_Settings) {
            return node;
        }

        if (!HasSetting(dqSource.Settings().Ref(), "extraColumns")) {
            return node;
        }

        if (!HasSetting(dqSource.Settings().Ref(), "prunedPaths")) {
            return CalculatePrunedPaths(flatMap, ctx, State_->Types);
        }

        return node;
    }

private:
    const TS3State::TPtr State_;
};

}

THolder<IGraphTransformer> CreateS3LogicalOptProposalTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3LogicalOptProposalTransformer>(state);
}

} // namespace NYql

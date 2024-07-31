#include "yql_yt_phy_opt.h"
#include "yql_yt_phy_opt_helper.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_join_impl.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>

#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;
using namespace NPrivate;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::EquiJoin(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
        return node;
    }

    auto equiJoin = node.Cast<TCoEquiJoin>();

    TMaybeNode<TYtDSink> dataSink;
    TString usedCluster;
    for (size_t i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
        auto list = equiJoin.Arg(i).Cast<TCoEquiJoinInput>().List();
        if (auto maybeExtractMembers = list.Maybe<TCoExtractMembers>()) {
            list = maybeExtractMembers.Cast().Input();
        }
        if (auto maybeFlatMap = list.Maybe<TCoFlatMapBase>()) {
            TSyncMap syncList;
            if (!IsYtCompleteIsolatedLambda(maybeFlatMap.Cast().Lambda().Ref(), syncList, usedCluster, true, false)) {
                return node;
            }
            list = maybeFlatMap.Cast().Input();
        }
        if (!IsYtProviderInput(list)) {
            TSyncMap syncList;
            if (!IsYtCompleteIsolatedLambda(list.Ref(), syncList, usedCluster, true, false)) {
                return node;
            }
            continue;
        }

        if (!dataSink) {
            dataSink = GetDataSink(list, ctx);
        }
        auto cluster = ToString(GetClusterName(list));
        if (!UpdateUsedCluster(usedCluster, cluster)) {
            return node;
        }
    }

    if (!dataSink) {
        return node;
    }

    THashMap<TStringBuf, std::pair<TVector<TStringBuf>, ui32>> tableSortKeysUsage =
        CollectTableSortKeysUsage(State_, equiJoin);

    // label -> join keys
    THashMap<TStringBuf, THashSet<TStringBuf>> tableKeysMap =
        CollectEquiJoinKeyColumnsByLabel(equiJoin.Arg(equiJoin.ArgCount() - 2).Ref());

    TNodeOnNodeOwnedMap updatedInputs;
    TExprNode::TListType sections;
    TExprNode::TListType premaps;
    TSyncMap worldList;
    for (size_t i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
        auto joinInput = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
        auto list = joinInput.List();

        TMaybeNode<TCoLambda> premapLambda;
        TExprNode::TPtr extractedMembers;

        auto listStepForward = list;
        if (auto maybeExtractMembers = listStepForward.Maybe<TCoExtractMembers>()) {
            extractedMembers = maybeExtractMembers.Cast().Members().Ptr();
            listStepForward = maybeExtractMembers.Cast().Input();
        }

        if (auto maybeFlatMap = listStepForward.Maybe<TCoFlatMapBase>()) {
            auto flatMap = maybeFlatMap.Cast();
            if (IsLambdaSuitableForPullingIntoEquiJoin(flatMap, joinInput.Scope().Ref(), tableKeysMap, extractedMembers.Get())) {
                if (!IsYtCompleteIsolatedLambda(flatMap.Lambda().Ref(), worldList, usedCluster, true, false)) {
                    return node;
                }

                auto maybeLambda = CleanupWorld(flatMap.Lambda(), ctx);
                if (!maybeLambda) {
                    return {};
                }

                auto lambda = ctx.DeepCopyLambda(maybeLambda.Cast().Ref());
                if (!extractedMembers) {
                    premapLambda = lambda;
                    YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Collected input #" << i << " as premap";
                } else {
                    premapLambda = ctx.Builder(lambda->Pos())
                        .Lambda()
                            .Param("item")
                            .Callable("ExtractMembers")
                                .Apply(0, lambda)
                                    .With(0, "item")
                                .Seal()
                                .Add(1, extractedMembers)
                            .Seal()
                        .Seal()
                        .Build();
                    YQL_CLOG(INFO, ProviderYt) << __FUNCTION__ << ": Collected input #" << i << " as premap with extract members";
                }

                list = flatMap.Input();
            }
        }

        TExprNode::TPtr section;
        if (!IsYtProviderInput(list)) {
            auto& newSection = updatedInputs[list.Raw()];
            if (newSection) {
                section = Build<TYtSection>(ctx, list.Pos())
                    .InitFrom(TYtSection(newSection))
                    .Settings()
                        .Add()
                            .Name()
                                .Value(ToString(EYtSettingType::JoinLabel))
                            .Build()
                            .Value(joinInput.Scope())
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
            else {
                TSyncMap syncList;
                if (!IsYtCompleteIsolatedLambda(list.Ref(), syncList, usedCluster, true, false)) {
                    return node;
                }

                const TStructExprType* outItemType = nullptr;
                if (auto type = GetSequenceItemType(list, false, ctx)) {
                    if (!EnsurePersistableType(list.Pos(), *type, ctx)) {
                        return {};
                    }
                    outItemType = type->Cast<TStructExprType>();
                } else {
                    return {};
                }

                TYtOutTableInfo outTable(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
                outTable.RowSpec->SetConstraints(list.Ref().GetConstraintSet());
                outTable.SetUnique(list.Ref().GetConstraint<TDistinctConstraintNode>(), list.Pos(), ctx);

                auto cleanup = CleanupWorld(list, ctx);
                if (!cleanup) {
                    return {};
                }

                section = newSection = Build<TYtSection>(ctx, list.Pos())
                    .Paths()
                        .Add()
                            .Table<TYtOutput>()
                                .Operation<TYtFill>()
                                    .World(ApplySyncListToWorld(ctx.NewWorld(list.Pos()), syncList, ctx))
                                    .DataSink(dataSink.Cast())
                                    .Content(MakeJobLambdaNoArg(cleanup.Cast(), ctx))
                                    .Output()
                                        .Add(outTable.ToExprNode(ctx, list.Pos()).Cast<TYtOutTable>())
                                    .Build()
                                    .Settings(GetFlowSettings(list.Pos(), *State_, ctx))
                                .Build()
                                .OutIndex().Value(0U).Build()
                            .Build()
                            .Columns<TCoVoid>().Build()
                            .Ranges<TCoVoid>().Build()
                            .Stat<TCoVoid>().Build()
                        .Build()
                    .Build()
                    .Settings()
                        .Add()
                            .Name()
                                .Value(ToString(EYtSettingType::JoinLabel))
                            .Build()
                            .Value(joinInput.Scope())
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
        }
        else {
            auto settings = Build<TCoNameValueTupleList>(ctx, list.Pos())
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::JoinLabel))
                    .Build()
                    .Value(joinInput.Scope())
                .Build()
                .Done();

            TExprNode::TPtr world;
            if (auto maybeRead = list.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
                auto read = maybeRead.Cast();
                if (read.World().Ref().Type() != TExprNode::World) {
                    world = read.World().Ptr();
                }
            }

            bool makeUnordered = false;
            if (ctx.IsConstraintEnabled<TSortedConstraintNode>() && joinInput.Scope().Ref().IsAtom()) {
                makeUnordered = (0 == tableSortKeysUsage[joinInput.Scope().Ref().Content()].second);
            }

            auto sectionList = ConvertInputTable(list, ctx, TConvertInputOpts().Settings(settings).MakeUnordered(makeUnordered));
            YQL_ENSURE(sectionList.Size() == 1, "EquiJoin input should contain exactly one section, but has: " << sectionList.Size());
            bool clearWorld = false;
            auto sectionNode = sectionList.Item(0);

            if (NYql::HasSetting(sectionNode.Settings().Ref(), EYtSettingType::Sample)) {
                auto scheme = list.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();

                auto path = CopyOrTrivialMap(sectionNode.Pos(),
                    TExprBase(world ? world : ctx.NewWorld(sectionNode.Pos())),
                    dataSink.Cast(),
                    *scheme,
                    Build<TYtSection>(ctx, sectionNode.Pos())
                        .InitFrom(sectionNode)
                        .Settings(NYql::RemoveSettings(sectionNode.Settings().Ref(), EYtSettingType::StatColumns
                                | EYtSettingType::JoinLabel | EYtSettingType::Unordered, ctx))
                        .Done(),
                    {}, ctx, State_,
                    TCopyOrTrivialMapOpts().SetTryKeepSortness(!makeUnordered).SetSectionUniq(list.Ref().GetConstraint<TDistinctConstraintNode>()));

                clearWorld = true;

                sectionNode = Build<TYtSection>(ctx, sectionNode.Pos())
                    .Paths()
                        .Add(path)
                    .Build()
                    .Settings(NYql::RemoveSettings(sectionNode.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip |
                        EYtSettingType::Sample, ctx))
                    .Done();
            }

            if (!clearWorld && world) {
                worldList.emplace(world, worldList.size());
            }

            section = sectionNode.Ptr();
        }

        YQL_ENSURE(section);
        sections.push_back(section);
        auto premap = BuildYtEquiJoinPremap(list, premapLambda, ctx);
        if (!premap) {
            return {};
        }
        premaps.push_back(premap);
    }

    const TStructExprType* outItemType = nullptr;
    if (auto type = GetSequenceItemType(node, false, ctx)) {
        if (!EnsurePersistableType(node.Pos(), *type, ctx)) {
            return {};
        }
        outItemType = type->Cast<TStructExprType>();
    } else {
        return {};
    }

    auto parentsMap = getParents();
    YQL_ENSURE(parentsMap);
    auto joinOptions = CollectPreferredSortsForEquiJoinOutput(node, equiJoin.Arg(equiJoin.ArgCount() - 1).Ptr(), ctx, *parentsMap);

    const auto join = Build<TYtEquiJoin>(ctx, node.Pos())
        .World(ApplySyncListToWorld(ctx.NewWorld(node.Pos()), worldList, ctx))
        .DataSink(dataSink.Cast())
        .Input()
            .Add(sections)
        .Build()
        .Output()
            .Add(TYtOutTableInfo(outItemType, State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE)
                .ToExprNode(ctx, node.Pos()).Cast<TYtOutTable>())
        .Build()
        .Settings()
        .Build()
        .Joins(equiJoin.Arg(equiJoin.ArgCount() - 2))
        .JoinOptions(joinOptions)
        .Done();

    auto children = join.Ref().ChildrenList();
    children.reserve(children.size() + premaps.size());
    std::move(premaps.begin(), premaps.end(), std::back_inserter(children));
    return Build<TYtOutput>(ctx, node.Pos())
        .Operation(ctx.ChangeChildren(join.Ref(), std::move(children)))
        .OutIndex().Value(0U).Build()
        .Done();
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::EarlyMergeJoin(TExprBase node, TExprContext& ctx) const {
    if (State_->Configuration->JoinMergeTablesLimit.Get()) {
        auto equiJoin = node.Cast<TYtEquiJoin>();
        const auto tree = ImportYtEquiJoin(equiJoin, ctx);
        if (State_->Configuration->JoinMergeForce.Get() || tree->LinkSettings.ForceSortedMerge) {
            const auto rewriteStatus = RewriteYtEquiJoinLeaves(equiJoin, *tree, State_, ctx);
            switch (rewriteStatus.Level) {
                case TStatus::Repeat:
                    return node;
                case TStatus::Error:
                    return {};
                case TStatus::Ok:
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected rewrite status");
            }
            return ExportYtEquiJoin(equiJoin, *tree, ctx, State_);
        }
    }
    return node;
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::RuntimeEquiJoin(TExprBase node, TExprContext& ctx) const {
    auto equiJoin = node.Cast<TYtEquiJoin>();

    const bool tryReorder = State_->Types->CostBasedOptimizer != ECostBasedOptimizerType::Disable
        && equiJoin.Input().Size() > 2
        && !HasSetting(equiJoin.JoinOptions().Ref(), "cbo_passed");

    const bool waitAllInputs = State_->Configuration->JoinWaitAllInputs.Get().GetOrElse(false) || tryReorder;

    if (waitAllInputs) {
        for (auto section: equiJoin.Input()) {
            for (auto path: section.Paths()) {
                TYtPathInfo pathInfo(path);
                if (!pathInfo.Table->Stat) {
                    return node;
                }
            }
        }
    }

    const auto tree = ImportYtEquiJoin(equiJoin, ctx);
    if (tryReorder) {
        const auto optimizedTree = OrderJoins(tree, State_, ctx);
        if (optimizedTree != tree) {
            return ExportYtEquiJoin(equiJoin, *optimizedTree, ctx, State_);
        }
    }
    const auto rewriteStatus = RewriteYtEquiJoin(equiJoin, *tree, State_, ctx);

    switch (rewriteStatus.Level) {
        case TStatus::Repeat:
            YQL_ENSURE(!waitAllInputs);
            return node;
        case TStatus::Error:
            return {};
        case TStatus::Ok:
            break;
        default:
            YQL_ENSURE(false, "Unexpected rewrite status");
    }

    return ExportYtEquiJoin(equiJoin, *tree, ctx, State_);
}

}  // namespace NYql

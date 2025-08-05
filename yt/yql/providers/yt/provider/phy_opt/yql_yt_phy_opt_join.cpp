#include "yql_yt_phy_opt.h"
#include "yql_yt_phy_opt_helper.h"

#include <yt/yql/providers/yt/common/yql_configuration.h>
#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>
#include <yt/yql/providers/yt/provider/yql_yt_cbo_helpers.h>
#include <yt/yql/providers/yt/provider/yql_yt_join_impl.h>
#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>

#include <yql/essentials/core/yql_type_helpers.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NYql {

using namespace NNodes;
using namespace NPrivate;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::EquiJoin(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
        return node;
    }

    auto equiJoin = node.Cast<TCoEquiJoin>();

    TString runtimeCluster;
    const ERuntimeClusterSelectionMode selectionMode =
        State_->Configuration->RuntimeClusterSelection.Get().GetOrElse(DEFAULT_RUNTIME_CLUSTER_SELECTION);
    TVector<TString> inputClusters(equiJoin.ArgCount() - 2);
    bool hasYtInput = false;
    for (size_t i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
        auto list = equiJoin.Arg(i).Cast<TCoEquiJoinInput>().List();
        if (auto maybeExtractMembers = list.Maybe<TCoExtractMembers>()) {
            list = maybeExtractMembers.Cast().Input();
        }
        if (auto maybeFlatMap = list.Maybe<TCoFlatMapBase>()) {
            TSyncMap syncList;
            if (!IsYtCompleteIsolatedLambda(maybeFlatMap.Cast().Lambda().Ref(), syncList, inputClusters[i], false, selectionMode)) {
                return node;
            }
            list = maybeFlatMap.Cast().Input();
        }
        if (!IsYtProviderInput(list)) {
            TSyncMap syncList;
            if (!IsYtCompleteIsolatedLambda(list.Ref(), syncList, inputClusters[i], false, selectionMode)) {
                return node;
            }
        } else {
            hasYtInput = true;
            auto cluster = DeriveClusterFromInput(list, selectionMode);
            if (!cluster || !UpdateUsedCluster(inputClusters[i], *cluster, selectionMode)) {
                return node;
            }
        }
        if (inputClusters[i] && !UpdateUsedCluster(runtimeCluster, inputClusters[i], selectionMode)) {
            return node;
        }
    }

    if (!hasYtInput) {
        return node;
    }

    YQL_ENSURE(runtimeCluster);

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
                if (!IsYtCompleteIsolatedLambda(flatMap.Lambda().Ref(), worldList, false)) {
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
                if (!IsYtCompleteIsolatedLambda(list.Ref(), syncList, false)) {
                    return node;
                }

                const TStructExprType* outItemType = nullptr;
                if (auto type = GetSequenceItemType(list, false, ctx)) {
                    if (!EnsurePersistableType(list.Pos(), *type, ctx)) {
                        return {};
                    }
                    if (!EnsurePersistableYsonTypes(list.Pos(), *type, ctx, State_)) {
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

                TYtDSink dataSink = MakeDataSink(list.Pos(), inputClusters[i] ? inputClusters[i] : runtimeCluster, ctx);
                section = newSection = Build<TYtSection>(ctx, list.Pos())
                    .Paths()
                        .Add()
                            .Table<TYtOutput>()
                                .Operation<TYtFill>()
                                    .World(ApplySyncListToWorld(ctx.NewWorld(list.Pos()), syncList, ctx))
                                    .DataSink(dataSink)
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

                if (!NPrivate::EnsurePersistableYsonTypes(sectionNode.Pos(), *scheme, ctx, State_)) {
                    return {};
                }
                TYtDSink dataSink = MakeDataSink(list.Pos(), inputClusters[i] ? inputClusters[i] : runtimeCluster, ctx);
                auto path = CopyOrTrivialMap(sectionNode.Pos(),
                    TExprBase(world ? world : ctx.NewWorld(sectionNode.Pos())),
                    dataSink,
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
        if (!EnsurePersistableYsonTypes(node.Pos(), *type, ctx, State_)) {
            return {};
        }
        outItemType = type->Cast<TStructExprType>();
    } else {
        return {};
    }

    auto parentsMap = getParents();
    YQL_ENSURE(parentsMap);
    auto joinOptions = CollectPreferredSortsForEquiJoinOutput(node, equiJoin.Arg(equiJoin.ArgCount() - 1).Ptr(), ctx, *parentsMap);

    if (sections.size() > 2 && State_->Configuration->ReportEquiJoinStats.Get().GetOrElse(DEFAULT_REPORT_EQUIJOIN_STATS)) {
        joinOptions = AddSetting(*joinOptions, joinOptions->Pos(), "multiple_joins", {}, ctx);
        with_lock(State_->StatisticsMutex) {
            State_->Statistics[Max<ui32>()].Entries.emplace_back("YtEquiJoin_MultipleCount", 0, 0, 0, 0, 1);
        }
    }

    const auto join = Build<TYtEquiJoin>(ctx, node.Pos())
        .World(ApplySyncListToWorld(ctx.NewWorld(node.Pos()), worldList, ctx))
        .DataSink(MakeDataSink(node.Pos(), runtimeCluster, ctx))
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

        const bool waitAllInputs = State_->Configuration->JoinWaitAllInputs.Get().GetOrElse(false);
        if (waitAllInputs && !AreJoinInputsReady(equiJoin)) {
            return node;
        }

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

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::AddPruneKeys(TExprBase node, TExprContext& ctx) const {
    // Analogue to flow2 core EquiJoin optimizer with PushPruneKeysIntoYtOperation optimizer
    static const char optName[] = "EmitPruneKeys";
    if (!IsOptimizerEnabled<optName>(*State_->Types) || IsOptimizerDisabled<optName>(*State_->Types)) {
        return node;
    }
    auto equiJoin = node.Cast<TYtEquiJoin>();
    if (HasSetting(equiJoin.JoinOptions().Ref(), "prune_keys_added")) {
        return node;
    }

    THashMap<TStringBuf, THashSet<TStringBuf>> columnsForPruneKeysExtractor;
    GetPruneKeysColumnsForJoinLeaves(equiJoin.Joins().Cast<TCoEquiJoinTuple>(), columnsForPruneKeysExtractor);

    TExprNode::TListType children;
    bool hasChanges = false;

    for (const auto& input : equiJoin.Input()) {
        auto section = input.Cast<TYtSection>();

        auto joinLabel = NYql::GetSetting(section.Settings().Ref(), EYtSettingType::JoinLabel);
        if (!joinLabel) {
            children.push_back(input.Ptr());
            continue;
        }

        bool hasTable = false;
        THashSet<TString> columns;
        if (joinLabel->Child(1)->IsAtom()) {
            if (auto itemNames = columnsForPruneKeysExtractor.find(joinLabel->Child(1)->Content());
                itemNames != columnsForPruneKeysExtractor.end() && !itemNames->second.empty()) {
                hasTable = true;
                for (const auto& elem : itemNames->second) {
                    columns.insert(TString(elem));
                }
            }
        } else {
            for (const auto& child : joinLabel->Child(1)->Children()) {
                if (auto itemNames = columnsForPruneKeysExtractor.find(child->Content());
                    itemNames != columnsForPruneKeysExtractor.end() && !itemNames->second.empty()) {
                    hasTable = true;
                    for (const auto& elem : itemNames->second) {
                        columns.insert(FullColumnName(itemNames->first, elem));
                    }
                }
            }
        }

        if (!hasTable) {
            children.push_back(input.Ptr());
            continue;
        }

        if (IsAlreadyDistinct(section.Ref(), columns)) {
            children.push_back(input.Ptr());
            continue;
        }
        bool isOrdered = IsOrdered(section.Ref(), columns);

        auto cluster = GetClusterFromSection(section);
        YQL_ENSURE(cluster);

        auto outItemType = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        auto inputSection = Build<TYtSection>(ctx, section.Pos())
            .InitFrom(section)
            .Settings(NYql::RemoveSettings(section.Settings().Ref(), EYtSettingType::JoinLabel | EYtSettingType::StatColumns, ctx))
            .Done();

        auto pruneKeysCallable = isOrdered ? "PruneAdjacentKeys" : "PruneKeys";
        YQL_CLOG(DEBUG, Core) << "Add " << pruneKeysCallable << " to YtEquiJoin input";

        auto map = BuildMapForPruneKeys(
            section,
            MakePruneKeysExtractorLambda(node.Ref(), columns, ctx),
            isOrdered,
            cluster,
            ctx.NewWorld(section.Pos()),
            Build<TYtSectionList>(ctx, section.Pos())
                .Add(inputSection)
                .Done(),
            outItemType,
            ctx,
            State_);

        children.push_back(Build<TYtSection>(ctx, section.Pos())
            .Paths()
                .Add()
                    .Table<TYtOutput>()
                        .InitFrom(map.Cast<TYtOutput>())
                    .Build()
                    .Columns<TCoVoid>().Build()
                    .Ranges<TCoVoid>().Build()
                    .Stat<TCoVoid>().Build()
                .Build()
            .Build()
            .Settings(section.Settings())
            .Done()
            .Ptr());
        hasChanges = true;
    }

    if (!hasChanges) {
        return node;
    }

    auto result = ctx.ChangeChild(
        node.Ref(),
        TYtEquiJoin::idx_Input,
        Build<TYtSectionList>(ctx, equiJoin.Input().Pos())
            .Add(children)
            .Done().Ptr());

    result = ctx.ChangeChild(
        *result,
        TYtEquiJoin::idx_JoinOptions,
        AddSetting(
            equiJoin.JoinOptions().Ref(),
            equiJoin.JoinOptions().Pos(),
            "prune_keys_added",
            nullptr,
            ctx));

    return TExprBase(result);
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::RuntimeEquiJoin(TExprBase node, TExprContext& ctx) const {
    auto equiJoin = node.Cast<TYtEquiJoin>();

    const bool tryReorder = State_->Types->CostBasedOptimizer != ECostBasedOptimizerType::Disable
        && equiJoin.Input().Size() > 2
        && !HasSetting(equiJoin.JoinOptions().Ref(), "cbo_passed");

    const bool waitAllInputs = State_->Configuration->JoinWaitAllInputs.Get().GetOrElse(false) || tryReorder;
    if (waitAllInputs && !AreJoinInputsReady(equiJoin)) {
        return node;
    }

    const auto tree = ImportYtEquiJoin(equiJoin, ctx);
    if (tryReorder) {
        YQL_CLOG(INFO, ProviderYt) << "Collecting cbo stats for equiJoin";
        auto collectStatus = CollectCboStats(*tree, State_, ctx);
        if (collectStatus == TStatus::Repeat) {
            return ExportYtEquiJoin(equiJoin, *tree, ctx, State_);
        }

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

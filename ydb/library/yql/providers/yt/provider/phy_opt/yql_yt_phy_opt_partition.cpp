#include "yql_yt_phy_opt.h"
#include "yql_yt_phy_opt_helper.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>

#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NYql {

using namespace NNodes;
using namespace NPrivate;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::PartitionByKey(TExprBase node, TExprContext& ctx) const {
    if (State_->Types->EvaluationInProgress || State_->PassiveExecution) {
        return node;
    }

    auto partByKey = node.Cast<TCoPartitionByKeyBase>();

    TExprBase input = partByKey.Input();
    TCoLambda keySelectorLambda = partByKey.KeySelectorLambda();
    TCoLambda handlerLambda = partByKey.ListHandlerLambda();

    if (!IsYtProviderInput(input, true)) {
        return node;
    }

    auto outItemType = SilentGetSequenceItemType(handlerLambda.Body().Ref(), true);
    if (!outItemType || !outItemType->IsPersistable()) {
        return node;
    }

    auto cluster = TString{GetClusterName(input)};
    TSyncMap syncList;
    if (!IsYtCompleteIsolatedLambda(keySelectorLambda.Ref(), syncList, cluster, false)
        || !IsYtCompleteIsolatedLambda(handlerLambda.Ref(), syncList, cluster, false)) {
        return node;
    }

    const auto inputItemType = GetSequenceItemType(input, true, ctx);
    if (!inputItemType) {
        return {};
    }
    const bool multiInput = (inputItemType->GetKind() == ETypeAnnotationKind::Variant);
    bool useSystemColumns = State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS);
    const bool useNativeDescSort = State_->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);

    TVector<TYtPathInfo::TPtr> inputPaths = GetInputPaths(input);
    bool needMap = AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) {
        return path->RequiresRemap();
    });

    bool forceMapper = false;
    if (auto maybeRead = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
        forceMapper = AnyOf(maybeRead.Cast().Input(), [] (const TYtSection& section) {
            return NYql::HasSetting(section.Settings().Ref(), EYtSettingType::SysColumns);
        });
    }

    if (!multiInput) {
        const ui64 nativeTypeFlags = State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES)
            ? GetNativeYtTypeFlags(*inputItemType->Cast<TStructExprType>())
            : 0ul;

        TMaybe<NYT::TNode> firstNativeType;
        if (!inputPaths.empty()) {
            firstNativeType = inputPaths.front()->GetNativeYtType();
        }

        forceMapper = forceMapper || AnyOf(inputPaths, [nativeTypeFlags, firstNativeType] (const TYtPathInfo::TPtr& path) {
            return nativeTypeFlags != path->GetNativeYtTypeFlags()
                || firstNativeType != path->GetNativeYtType();
        });
    }

    bool useExplicitColumns = AnyOf(inputPaths, [] (const TYtPathInfo::TPtr& path) {
        return !path->Table->IsTemp
            || (path->Table->RowSpec && path->Table->RowSpec->HasAuxColumns());
    });

    TKeySelectorBuilder builder(node.Pos(), ctx, useNativeDescSort, inputItemType);
    builder.ProcessKeySelector(keySelectorLambda.Ptr(), {}, true);

    TVector<std::pair<TString, bool>> reduceByColumns = builder.ForeignSortColumns();
    TVector<std::pair<TString, bool>> sortByColumns;

    if (!partByKey.SortDirections().Maybe<TCoVoid>()) {
        TExprBase sortDirections = partByKey.SortDirections();
        if (!IsConstExpSortDirections(sortDirections)) {
            return node;
        }

        TCoLambda sortKeySelectorLambda = partByKey.SortKeySelectorLambda().Cast<TCoLambda>();
        if (!IsYtCompleteIsolatedLambda(sortKeySelectorLambda.Ref(), syncList, cluster, false)) {
            return node;
        }

        builder.ProcessKeySelector(sortKeySelectorLambda.Ptr(), sortDirections.Ptr());
        sortByColumns = builder.ForeignSortColumns();
    }

    TExprBase mapper = Build<TCoVoid>(ctx, node.Pos()).Done();
    needMap = needMap || builder.NeedMap();

    bool hasInputSampling = false;
    // Read sampling settings from the first section only, because all sections should have the same sampling settings
    if (auto maybeReadSettings = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>().Input().Item(0).Settings()) {
        hasInputSampling = NYql::HasSetting(maybeReadSettings.Ref(), EYtSettingType::Sample);
    }

    bool canUseReduce = !needMap;
    if (canUseReduce) {
        TVector<std::pair<TString, bool>> sortPrefix;
        for (auto& pathInfo: inputPaths) {
            if (pathInfo->Table->IsUnordered
                || !pathInfo->Table->RowSpec
                || !pathInfo->Table->RowSpec->IsSorted()
                || pathInfo->Table->RowSpec->SortedBy.size() < builder.Columns().size())
            {
                canUseReduce = false;
                break;
            }
            if (sortPrefix.empty()) {
                // reduceBy columns can be in any order, with any ascending
                THashMap<TString, bool> partColumnSet(reduceByColumns.begin(), reduceByColumns.end());
                auto sortedBy = pathInfo->Table->RowSpec->GetForeignSort();
                const bool equalReduceByPrefix = AllOf(sortedBy.begin(), sortedBy.begin() + reduceByColumns.size(),
                    [&partColumnSet](const std::pair<TString, bool>& c) {
                        return partColumnSet.contains(c.first);
                    });

                // sortBy suffix should exactly match
                const bool equalSortBySuffix = equalReduceByPrefix && (sortByColumns.empty()
                    || std::equal(sortByColumns.begin() + reduceByColumns.size(), sortByColumns.end(), sortedBy.begin() + reduceByColumns.size()));

                if (equalSortBySuffix) {
                    sortPrefix.assign(sortedBy.begin(), sortedBy.begin() + builder.Columns().size());
                    // All other tables should have the same sort order as the first one
                } else {
                    canUseReduce = false;
                    break;
                }
            } else {
                auto sortedBy = pathInfo->Table->RowSpec->GetForeignSort();
                if (!std::equal(sortPrefix.begin(), sortPrefix.end(), sortedBy.begin())) {
                    canUseReduce = false;
                    break;
                }
            }
        }
        if (canUseReduce) {
            const auto reduceBySize = reduceByColumns.size();
            reduceByColumns.assign(sortPrefix.begin(), sortPrefix.begin() + reduceBySize);
            if (!sortByColumns.empty()) {
                sortByColumns = std::move(sortPrefix);
            }
        }
    }

    const bool canUseMapInsteadOfReduce = keySelectorLambda.Body().Ref().IsComplete() &&
        partByKey.SortDirections().Maybe<TCoVoid>() &&
        State_->Configuration->PartitionByConstantKeysViaMap.Get().GetOrElse(DEFAULT_PARTITION_BY_CONSTANT_KEYS_VIA_MAP);

    if (canUseMapInsteadOfReduce) {
        YQL_ENSURE(!canUseReduce);
        YQL_ENSURE(sortByColumns.empty());
        useSystemColumns = false;
    }

    auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, node.Pos());
    if (!canUseMapInsteadOfReduce) {
        settingsBuilder
            .Add()
                .Name().Value(ToString(EYtSettingType::ReduceBy)).Build()
                .Value(TExprBase(ToColumnPairList(reduceByColumns, node.Pos(), ctx)))
            .Build();
    }

    if (!sortByColumns.empty()) {
        settingsBuilder
            .Add()
                .Name().Value(ToString(EYtSettingType::SortBy)).Build()
                .Value(TExprBase(ToColumnPairList(sortByColumns, node.Pos(), ctx)))
            .Build();
    }
    if (!canUseReduce && multiInput) {
        needMap = true; // YtMapReduce with empty mapper doesn't support table indices
    }

    bool useReduceFlow = State_->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW);
    bool useMapFlow = useReduceFlow;

    const bool newPartsByKeys = bool(partByKey.Maybe<TCoPartitionsByKeys>());

    // Convert reduce output to stream
    if (newPartsByKeys) {
        if (useSystemColumns) {
            TNodeSet nodesToOptimize;
            TProcessedNodesSet processedNodes;
            auto arg = handlerLambda.Args().Arg(0).Ptr();
            VisitExpr(handlerLambda.Body().Ptr(), [&nodesToOptimize, &processedNodes, arg](const TExprNode::TPtr& node) {
                if (TMaybeNode<TCoChopper>(node).GroupSwitch().Body().Maybe<TCoIsKeySwitch>()) {
                    if (IsDepended(node->Head(), *arg)) {
                        nodesToOptimize.insert(node.Get());
                    }
                }
                else if (TMaybeNode<TCoCondense1>(node).SwitchHandler().Body().Maybe<TCoIsKeySwitch>()) {
                    if (IsDepended(node->Head(), *arg)) {
                        nodesToOptimize.insert(node.Get());
                    }
                }
                else if (TMaybeNode<TCoCondense>(node).SwitchHandler().Body().Maybe<TCoIsKeySwitch>()) {
                    if (IsDepended(node->Head(), *arg)) {
                        nodesToOptimize.insert(node.Get());
                    }
                }
                else if (TYtOutput::Match(node.Get())) {
                    // Stop traversing dependent operations
                    processedNodes.insert(node->UniqueId());
                    return false;
                }
                return true;
            });

            if (!nodesToOptimize.empty()) {
                TOptimizeExprSettings settings(State_->Types);
                settings.ProcessedNodes = &processedNodes; // Prevent optimizer to go deeper than current operation
                TExprNode::TPtr newBody = handlerLambda.Body().Ptr();
                auto status = OptimizeExpr(newBody, newBody, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                    if (nodesToOptimize.find(node.Get()) == nodesToOptimize.end()) {
                        return node;
                    }

                    if (auto maybeChopper = TMaybeNode<TCoChopper>(node)) {
                        auto chopper = maybeChopper.Cast();

                        auto chopperSwitch = ctx.Builder(chopper.GroupSwitch().Pos())
                            .Lambda()
                                .Param("key")
                                .Param("item")
                                .Callable("SqlExtractKey")
                                    .Arg(0, "item")
                                    .Lambda(1)
                                        .Param("row")
                                        .Callable("Member")
                                            .Arg(0, "row")
                                            .Atom(1, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Build();

                        TExprNode::TPtr chopperHandler;
                        TExprNode::TPtr chopperKeyExtract;
                        if (!canUseReduce && multiInput) {
                            chopperKeyExtract = ctx.Builder(chopper.Handler().Pos())
                                .Lambda()
                                    .Param("item")
                                    .Apply(chopper.KeyExtractor().Ptr())
                                        .With(0)
                                            .Callable("Member")
                                                .Arg(0, "item")
                                                .Atom(1, "_yql_original_row", TNodeFlags::Default)
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal()
                                .Build();

                            chopperHandler = ctx.Builder(chopper.Handler().Pos())
                                .Lambda()
                                    .Param("key")
                                    .Param("group")
                                    .Apply(chopper.Handler().Ptr())
                                        .With(0, "key")
                                        .With(1)
                                            .Callable("Map")
                                                .Arg(0, "group")
                                                .Lambda(1)
                                                    .Param("row")
                                                    .Callable("Member")
                                                        .Arg(0, "row")
                                                        .Atom(1, "_yql_original_row", TNodeFlags::Default)
                                                    .Seal()
                                                .Seal()
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal()
                                .Build();
                        } else {
                            chopperKeyExtract = ctx.Builder(chopper.Handler().Pos())
                                .Lambda()
                                    .Param("item")
                                    .Apply(chopper.KeyExtractor().Ptr())
                                        .With(0)
                                            .Callable("RemovePrefixMembers")
                                                .Arg(0, "item")
                                                .List(1)
                                                    .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                                .Seal()
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal()
                                .Build();

                            chopperHandler = ctx.Builder(chopper.Handler().Pos())
                                .Lambda()
                                    .Param("key")
                                    .Param("group")
                                    .Apply(chopper.Handler().Ptr())
                                        .With(0, "key")
                                        .With(1)
                                            .Callable("RemovePrefixMembers")
                                                .Arg(0, "group")
                                                .List(1)
                                                    .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                                .Seal()
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal()
                                .Build();
                        }

                        TNodeOnNodeOwnedMap deepClones;
                        return Build<TCoChopper>(ctx, chopper.Pos())
                            .Input(chopper.Input())
                            .KeyExtractor(chopperKeyExtract)
                            .GroupSwitch(chopperSwitch)
                            .Handler(chopperHandler)
                            .Done().Ptr();
                    }
                    else if (auto maybeCondense = TMaybeNode<TCoCondense1>(node)) {
                        auto condense = maybeCondense.Cast();
                        auto switchHandler = ctx.Builder(condense.SwitchHandler().Pos())
                            .Lambda()
                                .Param("item")
                                .Param("state")
                                .Callable("SqlExtractKey")
                                    .Arg(0, "item")
                                    .Lambda(1)
                                        .Param("row")
                                        .Callable("Member")
                                            .Arg(0, "row")
                                            .Atom(1, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Build();

                        TExprNode::TPtr initHandler;
                        TExprNode::TPtr updateHandler;

                        if (!canUseReduce && multiInput) {
                            initHandler = ctx.Builder(condense.InitHandler().Pos())
                                .Lambda()
                                    .Param("item")
                                    .Apply(condense.InitHandler().Ptr())
                                        .With(0)
                                            .Callable("Member")
                                                .Arg(0, "item")
                                                .Atom(1, "_yql_original_row", TNodeFlags::Default)
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal()
                                .Build();

                            updateHandler = ctx.Builder(condense.UpdateHandler().Pos())
                                .Lambda()
                                    .Param("item")
                                    .Param("state")
                                    .Apply(condense.UpdateHandler().Ptr())
                                        .With(0)
                                            .Callable("Member")
                                                .Arg(0, "item")
                                                .Atom(1, "_yql_original_row", TNodeFlags::Default)
                                            .Seal()
                                        .Done()
                                        .With(1, "state")
                                    .Seal()
                                .Seal()
                                .Build();
                        } else {
                            initHandler = ctx.Builder(condense.InitHandler().Pos())
                                .Lambda()
                                    .Param("item")
                                    .Apply(condense.InitHandler().Ptr())
                                        .With(0)
                                            .Callable("RemovePrefixMembers")
                                                .Arg(0, "item")
                                                .List(1)
                                                    .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                                .Seal()
                                            .Seal()
                                        .Done()
                                    .Seal()
                                .Seal()
                                .Build();

                            updateHandler = ctx.Builder(condense.UpdateHandler().Pos())
                                .Lambda()
                                    .Param("item")
                                    .Param("state")
                                    .Apply(condense.UpdateHandler().Ptr())
                                        .With(0)
                                            .Callable("RemovePrefixMembers")
                                                .Arg(0, "item")
                                                .List(1)
                                                    .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                                .Seal()
                                            .Seal()
                                        .Done()
                                        .With(1, "state")
                                    .Seal()
                                .Seal()
                                .Build();
                        }

                        return Build<TCoCondense1>(ctx, condense.Pos())
                            .Input(condense.Input())
                            .InitHandler(initHandler)
                            .SwitchHandler(switchHandler)
                            .UpdateHandler(updateHandler)
                            .Done().Ptr();
                    }
                    else if (auto maybeCondense = TMaybeNode<TCoCondense>(node)) {
                        auto condense = maybeCondense.Cast();

                        auto switchHandler = ctx.Builder(condense.SwitchHandler().Pos())
                            .Lambda()
                                .Param("item")
                                .Param("state")
                                .Callable("SqlExtractKey")
                                    .Arg(0, "item")
                                    .Lambda(1)
                                        .Param("row")
                                        .Callable("Member")
                                            .Arg(0, "row")
                                            .Atom(1, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Build();

                        TExprNode::TPtr updateHandler;
                        if (!canUseReduce && multiInput) {
                            updateHandler = ctx.Builder(condense.UpdateHandler().Pos())
                                .Lambda()
                                    .Param("item")
                                    .Param("state")
                                    .Apply(condense.UpdateHandler().Ptr())
                                        .With(0)
                                            .Callable("Member")
                                                .Arg(0, "item")
                                                .Atom(1, "_yql_original_row", TNodeFlags::Default)
                                            .Seal()
                                        .Done()
                                        .With(1, "state")
                                    .Seal()
                                .Seal()
                                .Build();
                        } else {
                            updateHandler = ctx.Builder(condense.UpdateHandler().Pos())
                                .Lambda()
                                    .Param("item")
                                    .Param("state")
                                    .Apply(condense.UpdateHandler().Ptr())
                                        .With(0)
                                            .Callable("RemovePrefixMembers")
                                                .Arg(0, "item")
                                                .List(1)
                                                    .Atom(0, YqlSysColumnKeySwitch, TNodeFlags::Default)
                                                .Seal()
                                            .Seal()
                                        .Done()
                                        .With(1, "state")
                                    .Seal()
                                .Seal()
                                .Build();
                        }

                        return Build<TCoCondense>(ctx, condense.Pos())
                            .Input(condense.Input())
                            .State(condense.State())
                            .SwitchHandler(switchHandler)
                            .UpdateHandler(updateHandler)
                            .Done().Ptr();
                    }

                    return node;
                }, ctx, settings);

                if (status.Level == TStatus::Error) {
                    return {};
                }

                if (status.Level == TStatus::Ok) {
                    useSystemColumns = false;
                }
                else {
                    handlerLambda = Build<TCoLambda>(ctx, handlerLambda.Pos())
                        .Args({TStringBuf("stream")})
                        .Body<TExprApplier>()
                            .Apply(TExprBase(newBody))
                            .With(handlerLambda.Args().Arg(0), TStringBuf("stream"))
                        .Build()
                        .Done();
                }
            }
            else {
                useSystemColumns = false;
            }
        }

        if (!useSystemColumns) {
            auto preReduceLambda = Build<TCoLambda>(ctx, handlerLambda.Pos())
                .Args({"stream"})
                .Body("stream")
                .Done();

            if (!canUseReduce && multiInput) {
                preReduceLambda = Build<TCoLambda>(ctx, handlerLambda.Pos())
                    .Args({"stream"})
                    .Body<TCoFlatMap>()
                        .Input("stream")
                        .Lambda()
                            .Args({"item"})
                            .Body<TCoJust>()
                                .Input<TCoMember>()
                                    .Struct("item")
                                    .Name()
                                        .Value("_yql_original_row")
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done();
            }

            handlerLambda = Build<TCoLambda>(ctx, handlerLambda.Pos())
                .Args({"stream"})
                .Body<TExprApplier>()
                    .Apply(handlerLambda)
                    .With<TExprApplier>(0)
                        .Apply(preReduceLambda)
                        .With(0, "stream")
                    .Build()
                .Build()
                .Done();
        }
    }
    else {
        TExprNode::TPtr groupSwitch;
        TExprNode::TPtr keyExtractor;
        TExprNode::TPtr handler;

        if (canUseMapInsteadOfReduce) {
            groupSwitch = Build<TCoLambda>(ctx, handlerLambda.Pos())
                .Args({"key", "item"})
                .Body<TCoBool>()
                    .Literal().Build("false")
                .Build()
                .Done().Ptr();
        } else if (useSystemColumns) {
            groupSwitch = Build<TCoLambda>(ctx, handlerLambda.Pos())
                .Args({"key", "item"})
                .Body<TCoSqlExtractKey>()
                    .Item("item")
                    .Extractor()
                        .Args({"row"})
                        .Body<TCoMember>()
                            .Struct("row")
                            .Name()
                                .Value(YqlSysColumnKeySwitch, TNodeFlags::Default)
                            .Build()
                        .Build()
                    .Build()
                .Build()
                .Done().Ptr();
        } else {
            groupSwitch = Build<TCoLambda>(ctx, handlerLambda.Pos())
                .Args({"key", "item"})
                .Body<TYtIsKeySwitch>()
                    .DependsOn()
                        .Input("item")
                    .Build()
                .Build()
                .Done().Ptr();
        }

        if (!canUseReduce && multiInput) {
            keyExtractor = Build<TCoLambda>(ctx, handlerLambda.Pos())
                .Args({"item"})
                .Body<TExprApplier>()
                    .Apply(keySelectorLambda)
                    .With<TCoMember>(0)
                        .Struct("item")
                        .Name()
                            .Value("_yql_original_row")
                        .Build()
                    .Build()
                .Build()
                .Done().Ptr();

            handler = Build<TCoLambda>(ctx, handlerLambda.Pos())
                .Args({"item"})
                .Body<TCoMember>()
                    .Struct("item")
                    .Name()
                        .Value("_yql_original_row")
                    .Build()
                .Build()
                .Done().Ptr();
        } else {
            keyExtractor = Build<TCoLambda>(ctx, handlerLambda.Pos())
                .Args({"item"})
                .Body<TExprApplier>()
                    .Apply(keySelectorLambda)
                    .With(0, "item")
                .Build()
                .Done().Ptr();

            if (canUseMapInsteadOfReduce) {
                handler = MakeIdentityLambda(handlerLambda.Pos(), ctx);
            } else {
                handler = Build<TCoLambda>(ctx, handlerLambda.Pos())
                    .Args({"item"})
                    .Body<TCoRemovePrefixMembers>()
                        .Input("item")
                        .Prefixes()
                            .Add()
                                .Value(YqlSysColumnKeySwitch)
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
            }
        }

        handlerLambda = Build<TCoLambda>(ctx, handlerLambda.Pos())
            .Args({"stream"})
            .Body<TExprApplier>()
                .Apply(handlerLambda)
                .With<TCoGroupingCore>(0)
                    .Input("stream")
                    .GroupSwitch(groupSwitch)
                    .KeyExtractor(keyExtractor)
                    .ConvertHandler(handler)
                .Build()
            .Build()
            .Done();
    }

    auto handlerLambdaCleanup = CleanupWorld(handlerLambda, ctx);
    if (!handlerLambdaCleanup) {
        return {};
    }

    const auto mapOutputType = needMap ? builder.MakeRemapType() : nullptr;
    auto reduceInputType = mapOutputType ? mapOutputType : inputItemType;
    if (useSystemColumns) {
        settingsBuilder
            .Add()
                .Name().Value(ToString(EYtSettingType::KeySwitch)).Build()
            .Build();

        if (ETypeAnnotationKind::Struct == reduceInputType->GetKind()) {
            auto items = reduceInputType->Cast<TStructExprType>()->GetItems();
            items.emplace_back(ctx.MakeType<TItemExprType>(YqlSysColumnKeySwitch, ctx.MakeType<TDataExprType>(EDataSlot::Bool)));
            reduceInputType = ctx.MakeType<TStructExprType>(items);
        }
    }

    TVector<TString> filterColumns;
    if (needMap) {
        if (auto maybeMapper = CleanupWorld(TCoLambda(builder.MakeRemapLambda()), ctx)) {
            mapper = maybeMapper.Cast();
        } else {
            return {};
        }

        if (builder.NeedMap() || multiInput) {
            if (multiInput) {
                filterColumns.emplace_back("_yql_original_row");
            }
            else {
                for (auto& item: inputItemType->Cast<TStructExprType>()->GetItems()) {
                    filterColumns.emplace_back(item->GetName());
                }
            }

            if (ETypeAnnotationKind::Struct == reduceInputType->GetKind()) {
                const std::unordered_set<std::string_view> set(filterColumns.cbegin(), filterColumns.cend());
                TVector<const TItemExprType*> items;
                items.reserve(set.size());
                for (const auto& item : reduceInputType->Cast<TStructExprType>()->GetItems())
                    if (const auto& name = item->GetName(); YqlSysColumnKeySwitch == name || set.cend() != set.find(name))
                        items.emplace_back(item);
                reduceInputType = ctx.MakeType<TStructExprType>(items);
            }
        }
    }

    auto reducer = newPartsByKeys ?
        MakeJobLambda<true>(handlerLambdaCleanup.Cast(), useReduceFlow, ctx):
        MakeJobLambda<false>(handlerLambdaCleanup.Cast(), useReduceFlow, ctx);

    if (useReduceFlow) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Flow))
                .Build()
            .Build();
    }

    if (canUseReduce) {
        auto reduce = Build<TYtReduce>(ctx, node.Pos())
            .World(ApplySyncListToWorld(GetWorld(input, {}, ctx).Ptr(), syncList, ctx))
            .DataSink(GetDataSink(input, ctx))
            .Input(ConvertInputTable(input, ctx))
            .Output()
                .Add(ConvertOutTables(node.Pos(), outItemType, ctx, State_, &partByKey.Ref().GetConstraintSet()))
            .Build()
            .Settings(settingsBuilder.Done())
            .Reducer(reducer)
            .Done();
        return WrapOp(reduce, ctx);
    }

    if (needMap && (builder.NeedMap() || multiInput) && !canUseMapInsteadOfReduce) {
        settingsBuilder
            .Add()
                .Name().Value(ToString(EYtSettingType::ReduceFilterBy)).Build()
                .Value(TExprBase(ToAtomList(filterColumns, node.Pos(), ctx)))
            .Build();
    }

    if (canUseMapInsteadOfReduce && !filterColumns.empty()) {
        reducer = Build<TCoLambda>(ctx, reducer.Pos())
            .Args({"input"})
            .Body<TExprApplier>()
                .Apply(reducer)
                .With<TCoMap>(0)
                    .Input("input")
                    .Lambda()
                        .Args({"item"})
                        .Body<TCoSelectMembers>()
                            .Input("item")
                            .Members(ToAtomList(filterColumns, node.Pos(), ctx))
                        .Build()
                    .Build()
                .Build()
            .Build()
            .Done();
    }

    bool unordered = ctx.IsConstraintEnabled<TSortedConstraintNode>();
    TExprBase world = GetWorld(input, {}, ctx);
    if (hasInputSampling) {

        if (forceMapper && !needMap) {
            mapper = Build<TCoLambda>(ctx, node.Pos()).Args({"stream"}).Body("stream").Done();
            needMap = true;
        }

        if (needMap) {
            input = Build<TYtOutput>(ctx, node.Pos())
                .Operation<TYtMap>()
                    .World(world)
                    .DataSink(GetDataSink(input, ctx))
                    .Input(ConvertInputTable(input, ctx, TConvertInputOpts().MakeUnordered(unordered)))
                    .Output()
                        .Add(ConvertOutTables(node.Pos(), mapOutputType ? mapOutputType : inputItemType, ctx, State_))
                    .Build()
                    .Settings(GetFlowSettings(node.Pos(), *State_, ctx))
                    .Mapper(MakeJobLambda<false>(mapper.Cast<TCoLambda>(), useMapFlow, ctx))
                .Build()
                .OutIndex().Value(0U).Build()
                .Done();

            mapper = Build<TCoVoid>(ctx, node.Pos()).Done();
            needMap = false;
            forceMapper = false;
        }
        else {
            TConvertInputOpts opts;
            if (useExplicitColumns) {
                opts.ExplicitFields(*inputItemType->Cast<TStructExprType>(), node.Pos(), ctx);
            }

            input = Build<TYtOutput>(ctx, node.Pos())
                .Operation<TYtMerge>()
                    .World(world)
                    .DataSink(GetDataSink(input, ctx))
                    .Input(ConvertInputTable(input, ctx, opts.MakeUnordered(unordered)))
                    .Output()
                        .Add(ConvertOutTables(node.Pos(), inputItemType, ctx, State_))
                    .Build()
                    .Settings()
                        .Add()
                            .Name()
                                .Value(ToString(EYtSettingType::ForceTransform))
                            .Build()
                        .Build()
                    .Build()
                .Build()
                .OutIndex().Value(0U).Build()
                .Done();
        }
        world = TExprBase(ctx.NewWorld(node.Pos()));
        unordered = false;
    }

    if (needMap) {
        if (multiInput) {
            input = Build<TYtOutput>(ctx, node.Pos())
                .Operation<TYtMap>()
                    .World(world)
                    .DataSink(GetDataSink(input, ctx))
                    .Input(ConvertInputTable(input, ctx, TConvertInputOpts().MakeUnordered(unordered)))
                    .Output()
                        .Add(ConvertOutTables(node.Pos(), mapOutputType, ctx, State_))
                    .Build()
                    .Settings(GetFlowSettings(node.Pos(), *State_, ctx))
                    .Mapper(MakeJobLambda<false>(mapper.Cast<TCoLambda>(), useMapFlow, ctx))
                .Build()
                .OutIndex().Value(0U).Build()
                .Done();

            mapper = Build<TCoVoid>(ctx, node.Pos()).Done();
            world = TExprBase(ctx.NewWorld(node.Pos()));
            unordered = false;
        } else {
            useMapFlow = useReduceFlow;
            mapper = MakeJobLambda<false>(mapper.Cast<TCoLambda>(), useMapFlow, ctx);
        }
    }

    if (canUseMapInsteadOfReduce) {
        settingsBuilder
            .Add()
                .Name().Value(ToString(EYtSettingType::JobCount)).Build()
                .Value(TExprBase(ctx.NewAtom(node.Pos(), 1u)))
            .Build();

        auto result = Build<TYtMap>(ctx, node.Pos())
            .World(ApplySyncListToWorld(world.Ptr(), syncList, ctx))
            .DataSink(GetDataSink(input, ctx))
            .Input(ConvertInputTable(input, ctx, TConvertInputOpts().MakeUnordered(unordered)))
            .Output()
                .Add(ConvertOutTables(node.Pos(), outItemType, ctx, State_, &partByKey.Ref().GetConstraintSet()))
            .Build()
            .Settings(settingsBuilder.Done())
            .Mapper(reducer)
            .Done();
        return WrapOp(result, ctx);
    }

    if (forceMapper && mapper.Maybe<TCoVoid>()) {
        mapper = MakeJobLambda<false>(Build<TCoLambda>(ctx, node.Pos()).Args({"stream"}).Body("stream").Done(), useMapFlow, ctx);
    }
    auto mapReduce = Build<TYtMapReduce>(ctx, node.Pos())
        .World(ApplySyncListToWorld(world.Ptr(), syncList, ctx))
        .DataSink(GetDataSink(input, ctx))
        .Input(ConvertInputTable(input, ctx, TConvertInputOpts().MakeUnordered(unordered)))
        .Output()
            .Add(ConvertOutTables(node.Pos(), outItemType, ctx, State_, &partByKey.Ref().GetConstraintSet()))
        .Build()
        .Settings(settingsBuilder.Done())
        .Mapper(mapper)
        .Reducer(reducer)
        .Done();
    return WrapOp(mapReduce, ctx);
}

}  // namespace NYql

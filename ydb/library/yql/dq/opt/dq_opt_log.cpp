#include "dq_opt_log.h"

#include "dq_opt.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_aggregate_expander.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_window.h>
#include <ydb/library/yql/core/yql_opt_match_recognize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/dq/integration/yql_dq_optimization.h>

using namespace NYql::NNodes;

namespace NYql::NDq {

TExprBase DqRewriteAggregate(TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typesCtx, bool compactForDistinct,
    bool usePhases, const bool useFinalizeByKey, const bool allowSpilling)
{
    if (!node.Maybe<TCoAggregateBase>()) {
        return node;
    }
    TAggregateExpander aggExpander(!typesCtx.IsBlockEngineEnabled() && !useFinalizeByKey,
        useFinalizeByKey, node.Ptr(), ctx, typesCtx, false, compactForDistinct, usePhases, allowSpilling);
    auto result = aggExpander.ExpandAggregate();
    YQL_ENSURE(result);

    return TExprBase(result);
}

// Take . Sort -> TopSort
// Take . Skip . Sort -> Take . Skip . TopSort
TExprBase DqRewriteTakeSortToTopSort(TExprBase node, TExprContext& ctx, const TParentsMap& parents) {
    if (!node.Maybe<TCoTake>()) {
        return node;
    }
    auto take = node.Cast<TCoTake>();

    if (!IsDqCompletePureExpr(take.Count())) {
        return node;
    }

    auto input = take.Input();

    auto maybeSkip = input.Maybe<TCoSkip>();
    if (maybeSkip) {
        input = maybeSkip.Cast().Input();

        if (!IsSingleConsumer(maybeSkip.Cast(), parents)) {
            return node;
        }

        if (!IsDqCompletePureExpr(maybeSkip.Cast().Count())) {
            return node;
        }
    }

    auto maybeExtractMembers = input.Maybe<TCoExtractMembers>();
    if (maybeExtractMembers) {
        input = maybeExtractMembers.Cast().Input();

        if (!IsSingleConsumer(maybeExtractMembers.Cast(), parents)) {
            return node;
        }
    }

    auto maybeSort = input.Maybe<TCoSort>();
    if (!maybeSort) {
        return node;
    }

    auto sort = maybeSort.Cast();
    if (!IsSingleConsumer(sort, parents)) {
        return node;
    }

    auto topSortCount = take.Count();
    if (maybeSkip) {
        topSortCount = Build<TCoAggrAdd>(ctx, node.Pos())
            .Left(take.Count())
            .Right(maybeSkip.Cast().Count())
            .Done();
    }

    TExprBase result = Build<TCoTopSort>(ctx, node.Pos())
        .Input(sort.Input())
        .KeySelectorLambda(sort.KeySelectorLambda())
        .SortDirections(sort.SortDirections())
        .Count(topSortCount)
        .Done();

    if (maybeSkip) {
        result = Build<TCoTake>(ctx, node.Pos())
            .Input<TCoSkip>()
                .Input(result)
                .Count(maybeSkip.Cast().Count())
                .Build()
            .Count(take.Count())
            .Done();
    }

    if (maybeExtractMembers) {
        result = Build<TCoExtractMembers>(ctx, node.Pos())
            .Input(result)
            .Members(maybeExtractMembers.Cast().Members())
            .Done();
    }

    return result;
}

/*
 * Enforce PARTITION COMPACT BY as it avoids generating join in favour of Fold1Map.
 */
TExprBase DqEnforceCompactPartition(TExprBase node, TExprList frames, TExprContext& ctx) {

    for (const auto &frameNode : frames.Ref().Children()) {
        YQL_ENSURE(TCoWinOnBase::Match(frameNode.Get()));

        auto frameSpec = frameNode->Child(0);
        if (frameSpec->Type() == TExprNode::List) {
            TVector<TExprBase> values;
            bool compact = false;

            for (const auto& setting : frameSpec->Children()) {
                const auto settingName = setting->Head().Content();
                if (settingName == "compact") {
                    compact = true;
                    break;
                }
                values.push_back(TExprBase(setting));
            }

            if (!compact) {
                auto newFrameSpec = Build<TExprList>(ctx, frameNode->Pos())
                    .Add(values)
                    .Add<TExprList>()
                        .Add<TCoAtom>()
                        .Value("compact")
                        .Build()
                    .Build()
                .Done();

                TNodeOnNodeOwnedMap replaces;
                replaces[frameNode->Child(0)] = newFrameSpec.Ptr();
                node = TExprBase(ctx.ReplaceNodes(node.Ptr(), replaces));
            }
        }
    }

    return node;
}

TExprBase DqExpandWindowFunctions(TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typesCtx, bool enforceCompact) {
    if (node.Maybe<TCoCalcOverWindowBase>() || node.Maybe<TCoCalcOverWindowGroup>()) {
        if (enforceCompact) {
            auto calcs = ExtractCalcsOverWindow(node.Ptr(), ctx);
            bool changed = false;
            for (auto& c : calcs) {
                TCoCalcOverWindowTuple win(c);
                auto enforced = DqEnforceCompactPartition(node, win.Frames(), ctx);
                changed = changed || (enforced.Raw() != node.Raw());
                node = enforced;
            }

            if (changed) {
                return node;
            }
        }

        return TExprBase(ExpandCalcOverWindow(node.Ptr(), ctx, typesCtx));
    } else {
        return node;
    }
}

static void CollectSinkStages(const NNodes::TDqQuery& dqQuery, THashSet<TExprNode::TPtr, TExprNode::TPtrHash>& sinkStages) {
    for (const auto& stage : dqQuery.SinkStages()) {
        sinkStages.insert(stage.Ptr());
    }
}

NNodes::TExprBase DqMergeQueriesWithSinks(NNodes::TExprBase dqQueryNode, TExprContext& ctx) {
    NNodes::TDqQuery dqQuery = dqQueryNode.Cast<NNodes::TDqQuery>();

    THashSet<TExprNode::TPtr, TExprNode::TPtrHash> sinkStages;
    CollectSinkStages(dqQuery, sinkStages);
    TOptimizeExprSettings settings{nullptr};
    settings.VisitLambdas = false;
    bool deletedDqQueryChild = false;
    TExprNode::TPtr newDqQueryNode;
    auto status = OptimizeExpr(dqQueryNode.Ptr(), newDqQueryNode, [&sinkStages, &deletedDqQueryChild](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        for (ui32 childIndex = 0; childIndex < node->ChildrenSize(); ++childIndex) {
            TExprNode* child = node->Child(childIndex);
            if (child->IsCallable(NNodes::TDqQuery::CallableName())) {
                NNodes::TDqQuery dqQueryChild(child);
                CollectSinkStages(dqQueryChild, sinkStages);
                deletedDqQueryChild = true;
                return ctx.ChangeChild(*node, childIndex, dqQueryChild.World().Ptr());
            }
        }
        return node;
    }, ctx, settings);
    YQL_ENSURE(status != IGraphTransformer::TStatus::Error, "Failed to merge DqQuery nodes: " << status);

    if (deletedDqQueryChild) {
        auto dqQueryBuilder = Build<TDqQuery>(ctx, dqQuery.Pos());
        dqQueryBuilder.World(newDqQueryNode->ChildPtr(TDqQuery::idx_World));

        auto sinkStagesBuilder = dqQueryBuilder.SinkStages();
        for (const TExprNode::TPtr& stage : sinkStages) {
            sinkStagesBuilder.Add(stage);
        }
        sinkStagesBuilder.Build();

        return dqQueryBuilder.Done();
    }
    return dqQueryNode;
}

NNodes::TExprBase DqFlatMapOverExtend(NNodes::TExprBase node, TExprContext& ctx)
{
    auto maybeFlatMap = node.Maybe<TCoFlatMapBase>();
    if (!maybeFlatMap) {
        return node;
    }
    auto flatMap = maybeFlatMap.Cast();
    if (!flatMap.Input().Maybe<TCoExtendBase>()) {
        return node;
    }

    bool hasDqConnection = false;;
    auto input = flatMap.Input();
    for (auto child: input.Ref().Children()) {
        hasDqConnection |= !!TExprBase{child}.Maybe<TDqConnection>();
    }

    if (!hasDqConnection) {
        return node;
    }

    const bool ordered = flatMap.Maybe<TCoOrderedFlatMap>() && !input.Maybe<TCoExtend>();
    TExprNode::TListType extendChildren;
    for (auto child: input.Ref().Children()) {
        extendChildren.push_back(ctx.Builder(child->Pos())
            .Callable(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
                .Add(0, child)
                .Add(1, flatMap.Lambda().Ptr())
            .Seal()
            .Build());
    }
    TStringBuf extendName = input.Maybe<TCoMerge>()
        ? TCoMerge::CallableName()
        : (ordered ? TCoOrderedExtend::CallableName() : TCoExtend::CallableName());

    auto res = ctx.NewCallable(node.Pos(), extendName, std::move(extendChildren));
    return TExprBase(res);
}

NNodes::TExprBase DqSqlInDropCompact(NNodes::TExprBase node, TExprContext& ctx) {
    auto maybeSqlIn = node.Maybe<TCoSqlIn>();
    if (!maybeSqlIn || !maybeSqlIn.Collection().Maybe<TDqConnection>().IsValid() || !maybeSqlIn.Options().IsValid()) {
        return node;
    }
    if (HasSetting(maybeSqlIn.Cast().Options().Ref(), "isCompact")) {
        return TExprBase(ctx.ChangeChild(
            maybeSqlIn.Cast().Ref(),
            TCoSqlIn::idx_Options,
            RemoveSetting(maybeSqlIn.Cast().Options().Ref(), "isCompact", ctx)));
    }
    return node;
}

NNodes::TExprBase DqReplicateFieldSubset(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& parentsMap) {
    auto maybeReplicate = node.Maybe<TDqReplicate>();
    if (!maybeReplicate) {
        return node;
    }
    auto replicate = maybeReplicate.Cast();

    auto structType = GetSeqItemType(*replicate.Input().Ref().GetTypeAnn()).Cast<TStructExprType>();

    TSet<TStringBuf> usedFields;
    for (auto expr: replicate.FreeArgs()) {
        auto lambda = expr.Cast<TCoLambda>();
        auto it = parentsMap.find(lambda.Args().Arg(0).Raw());
        // Argument is unused
        if (it == parentsMap.cend()) {
            continue;
        }

        for (auto parent: it->second) {
            if (auto maybeFlatMap = TMaybeNode<TCoFlatMapBase>(parent)) {
                auto flatMap = maybeFlatMap.Cast();
                TSet<TStringBuf> lambdaSubset;
                if (!HaveFieldsSubset(flatMap.Lambda().Body().Ptr(), flatMap.Lambda().Args().Arg(0).Ref(), lambdaSubset, parentsMap)) {
                    return node;
                }
                usedFields.insert(lambdaSubset.cbegin(), lambdaSubset.cend());
            }
            else if (auto maybeExtractMembers = TMaybeNode<TCoExtractMembers>(parent)) {
                auto extractMembers = maybeExtractMembers.Cast();
                for (auto member: extractMembers.Members()) {
                    usedFields.insert(member.Value());
                }
            }
            else if (!TCoDependsOn::Match(parent)) {
                return node;
            }

            if (usedFields.size() == structType->GetSize()) {
                return node;
            }
        }

        if (usedFields.size() == structType->GetSize()) {
            return node;
        }
    }

    if (usedFields.size() < structType->GetSize()) {
        return TExprBase(ctx.Builder(replicate.Pos())
            .Callable(TDqReplicate::CallableName())
                .Callable(0, TCoExtractMembers::CallableName())
                    .Add(0, replicate.Input().Ptr())
                    .List(1)
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            ui32 i = 0;
                            for (auto column : usedFields) {
                                parent.Atom(i++, column);
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    ui32 i = 1;
                    for (auto expr: replicate.FreeArgs()) {
                        parent.Add(i++, ctx.DeepCopyLambda(expr.Ref()));
                    }
                    return parent;
                })
            .Seal()
            .Build());
    }

    return node;
}

IGraphTransformer::TStatus DqWrapIO(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx, TTypeAnnotationContext& typesCtx, const TDqSettings& config) {
    TOptimizeExprSettings settings{&typesCtx};
    auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) {
        if (auto maybeRead = TMaybeNode<TCoRight>(node).Input()) {
            if (maybeRead.Raw()->ChildrenSize() > 1 && TCoDataSource::Match(maybeRead.Raw()->Child(1))) {
                auto dataSourceName = maybeRead.Raw()->Child(1)->Child(0)->Content();
                auto dataSource = typesCtx.DataSourceMap.FindPtr(dataSourceName);
                YQL_ENSURE(dataSource);
                if (auto dqIntegration = (*dataSource)->GetDqIntegration()) {
                    auto newRead = dqIntegration->WrapRead(config, maybeRead.Cast().Ptr(), ctx);
                    if (newRead.Get() != maybeRead.Raw()) {
                        return newRead;
                    }
                }
            }
        } else if (node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::World
            && !TCoCommit::Match(node.Get())
            && node->ChildrenSize() > 1
            && TCoDataSink::Match(node->Child(1))) {
            auto dataSinkName = node->Child(1)->Child(0)->Content();
            auto dataSink = typesCtx.DataSinkMap.FindPtr(dataSinkName);
            YQL_ENSURE(dataSink);
            if (auto dqIntegration = (*dataSink)->GetDqIntegration()) {
                return dqIntegration->RecaptureWrite(node, ctx);
            }
        }

        return node;
    }, ctx, settings);
    return status;
}

TExprBase DqExpandMatchRecognize(TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx) {
    YQL_ENSURE(node.Maybe<TCoMatchRecognize>(), "Expected MatchRecognize");
    return TExprBase(ExpandMatchRecognize(node.Ptr(), ctx, typeAnnCtx));
}

TMaybeNode<TExprBase> UnorderedOverDqReadWrap(TExprBase node, TExprContext& ctx, const std::function<const TParentsMap*()>& getParents, bool enableDqReplicate, TTypeAnnotationContext& typeAnnCtx) {
    const auto unordered = node.Cast<TCoUnorderedBase>();
    if (const auto maybeRead = unordered.Input().Maybe<TDqReadWrapBase>().Input()) {
        if (enableDqReplicate) {
            const TParentsMap* parentsMap = getParents();
            auto parentsIt = parentsMap->find(unordered.Input().Raw());
            YQL_ENSURE(parentsIt != parentsMap->cend());
            if (parentsIt->second.size() > 1) {
                return node;
            }
        }
        auto providerRead = maybeRead.Cast();
        if (auto dqOpt = GetDqOptCallback(providerRead, typeAnnCtx)) {
            auto updatedRead = dqOpt->ApplyUnordered(providerRead.Ptr(), ctx);
            if (!updatedRead) {
                return {};
            }
            if (updatedRead != providerRead.Ptr()) {
                return TExprBase(ctx.ChangeChild(unordered.Input().Ref(), TDqReadWrapBase::idx_Input, std::move(updatedRead)));
            }
        }
    }

    return node;
}

TMaybeNode<TExprBase> ExtractMembersOverDqReadWrap(TExprBase node, TExprContext& ctx, const std::function<const TParentsMap*()>& getParents, bool enableDqReplicate, TTypeAnnotationContext& typeAnnCtx) {
    auto extract = node.Cast<TCoExtractMembers>();
    if (const auto maybeRead = extract.Input().Maybe<TDqReadWrap>().Input()) {
        if (enableDqReplicate) {
            const TParentsMap* parentsMap = getParents();
            auto parentsIt = parentsMap->find(extract.Input().Raw());
            YQL_ENSURE(parentsIt != parentsMap->cend());
            if (parentsIt->second.size() > 1) {
                return node;
            }
        }
        auto providerRead = maybeRead.Cast();
        if (auto dqOpt = GetDqOptCallback(providerRead, typeAnnCtx)) {
            auto updatedRead = dqOpt->ApplyExtractMembers(providerRead.Ptr(), extract.Members().Ptr(), ctx);
            if (!updatedRead) {
                return {};
            }
            if (updatedRead != providerRead.Ptr()) {
                return TExprBase(ctx.ChangeChild(extract.Input().Ref(), TDqReadWrap::idx_Input, std::move(updatedRead)));
            }
        }
    }

    return node;
}

TMaybeNode<TExprBase> TakeOrSkipOverDqReadWrap(TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx) {
    auto countBase = node.Cast<TCoCountBase>();

    // TODO: support via precomputes
    if (!TCoIntegralCtor::Match(countBase.Count().Raw())) {
        return node;
    }

    if (const auto maybeRead = countBase.Input().Maybe<TDqReadWrapBase>().Input()) {
        auto providerRead = maybeRead.Cast();
        if (auto dqOpt = GetDqOptCallback(providerRead, typeAnnCtx)) {
            auto updatedRead = dqOpt->ApplyTakeOrSkip(providerRead.Ptr(), countBase.Ptr(), ctx);
            if (!updatedRead) {
                return {};
            }
            if (updatedRead != providerRead.Ptr()) {
                return TExprBase(ctx.ChangeChild(countBase.Input().Ref(), TDqReadWrapBase::idx_Input, std::move(updatedRead)));
            }
        }
    }

    return node;
}

TMaybeNode<TExprBase> ExtendOverDqReadWrap(TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx) {
    auto extend = node.Cast<TCoExtendBase>();
    const bool ordered = node.Maybe<TCoOrderedExtend>().IsValid();
    const TExprNode* flags = nullptr;
    const TExprNode* token = nullptr;
    bool first = true;
    std::unordered_map<IDqOptimization*, std::vector<std::pair<size_t, TExprNode::TPtr>>> readers;
    IDqOptimization* prevDqOpt = nullptr;
    for (size_t i = 0; i < extend.ArgCount(); ++i) {
        const auto child = extend.Arg(i);
        if (!TDqReadWrapBase::Match(child.Raw())) {
            prevDqOpt = nullptr;
            continue;
        }
        auto dqReadWrap = child.Cast<TDqReadWrapBase>();

        if (first) {
            flags = dqReadWrap.Flags().Raw();
            token = dqReadWrap.Token().Raw();
            first = false;
        } else if (flags != dqReadWrap.Flags().Raw() || token != dqReadWrap.Token().Raw()) {
            prevDqOpt = nullptr;
            continue;
        }
        IDqOptimization* dqOpt = GetDqOptCallback(dqReadWrap.Input(), typeAnnCtx);
        if (!dqOpt) {
            prevDqOpt = nullptr;
            continue;
        }
        if (ordered && prevDqOpt != dqOpt) {
            readers[dqOpt].assign(1, std::make_pair(i, dqReadWrap.Input().Ptr()));
        } else {
            readers[dqOpt].emplace_back(i, dqReadWrap.Input().Ptr());
        }
        prevDqOpt = dqOpt;
    }

    if (readers.empty() || AllOf(readers, [](const auto& item) { return item.second.size() < 2; })) {
        return node;
    }

    TExprNode::TListType newChildren = extend.Ref().ChildrenList();
    for (auto& [dqOpt, list]: readers) {
        if (list.size() > 1) {
            TExprNode::TListType inReaders;
            std::transform(list.begin(), list.end(), std::back_inserter(inReaders), [](const auto& item) { return item.second; });
            TExprNode::TListType outReaders = dqOpt->ApplyExtend(inReaders, ordered, ctx);
            if (outReaders.empty()) {
                return {};
            }
            if (inReaders == outReaders) {
                return node;
            }
            YQL_ENSURE(outReaders.size() <= inReaders.size());
            size_t i = 0;
            for (; i < outReaders.size(); ++i) {
                newChildren[list[i].first] = ctx.ChangeChild(*newChildren[list[i].first], TDqReadWrapBase::idx_Input, std::move(outReaders[i]));
            }
            for (; i < list.size(); ++i) {
                newChildren[list[i].first] = nullptr;
            }
        }
    }
    newChildren.erase(std::remove(newChildren.begin(), newChildren.end(), TExprNode::TPtr{}), newChildren.end());
    YQL_ENSURE(!newChildren.empty());
    if (newChildren.size() > 1) {
        return TExprBase(ctx.ChangeChildren(extend.Ref(), std::move(newChildren)));
    } else {
        return TExprBase(newChildren.front());
    }
}

TMaybeNode<TExprBase> DqReadWideWrapFieldSubset(TExprBase node, TExprContext& ctx, const std::function<const TParentsMap*()>& getParents, TTypeAnnotationContext& typeAnnCtx) {
    auto map = node.Cast<TCoMapBase>();

    if (const auto maybeRead = map.Input().Maybe<TDqReadWideWrap>().Input()) {
        const TParentsMap* parentsMap = getParents();
        auto parentsIt = parentsMap->find(map.Input().Raw());
        YQL_ENSURE(parentsIt != parentsMap->cend());
        if (parentsIt->second.size() > 1) {
            return node;
        }

        TDynBitMap unusedArgs;
        for (ui32 i = 0; i < map.Lambda().Args().Size(); ++i) {
            if (auto parentsIt = parentsMap->find(map.Lambda().Args().Arg(i).Raw()); parentsIt == parentsMap->cend() || parentsIt->second.empty()) {
                unusedArgs.Set(i);
            }
        }
        if (unusedArgs.Empty()) {
            return node;
        }

        auto providerRead = maybeRead.Cast();
        if (auto dqOpt = GetDqOptCallback(providerRead, typeAnnCtx)) {

            auto structType = GetSeqItemType(*providerRead.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1]).Cast<TStructExprType>();
            TExprNode::TListType newMembers;
            for (ui32 i = 0; i < map.Lambda().Args().Size(); ++i) {
                if (!unusedArgs.Get(i)) {
                    newMembers.push_back(ctx.NewAtom(providerRead.Pos(), structType->GetItems().at(i)->GetName()));
                }
            }

            auto updatedRead = dqOpt->ApplyExtractMembers(providerRead.Ptr(), ctx.NewList(providerRead.Pos(), std::move(newMembers)), ctx);
            if (!updatedRead) {
                return {};
            }
            if (updatedRead == providerRead.Ptr()) {
                return node;
            }

            TExprNode::TListType newArgs;
            TNodeOnNodeOwnedMap replaces;
            for (ui32 i = 0; i < map.Lambda().Args().Size(); ++i) {
                if (!unusedArgs.Get(i)) {
                    auto newArg = ctx.NewArgument(map.Lambda().Args().Arg(i).Pos(), map.Lambda().Args().Arg(i).Name());
                    newArgs.push_back(newArg);
                    replaces.emplace(map.Lambda().Args().Arg(i).Raw(), std::move(newArg));
                }
            }

            auto newLambda = ctx.NewLambda(
                map.Lambda().Pos(),
                ctx.NewArguments(map.Lambda().Args().Pos(), std::move(newArgs)),
                ctx.ReplaceNodes(GetLambdaBody(map.Lambda().Ref()), replaces));

            return Build<TCoMapBase>(ctx, map.Pos())
                .CallableName(map.CallableName())
                .Input<TDqReadWideWrap>()
                    .InitFrom(map.Input().Cast<TDqReadWideWrap>())
                    .Input(updatedRead)
                .Build()
                .Lambda(newLambda)
                .Done();
        }
    }
    return node;
}

TMaybeNode<TExprBase> DqReadWrapByProvider(TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx) {
    auto providerRead = node.Cast<TDqReadWrapBase>().Input();
    if (auto dqOpt = GetDqOptCallback(providerRead, typeAnnCtx)) {
        auto updatedRead = dqOpt->RewriteRead(providerRead.Ptr(), ctx);
        if (!updatedRead) {
            return {};
        }
        if (updatedRead != providerRead.Ptr()) {
            return TExprBase(ctx.ChangeChild(node.Ref(), TDqReadWrapBase::idx_Input, std::move(updatedRead)));
        }
    }
    return node;
}

TMaybeNode<TExprBase> ExtractMembersOverDqReadWrapMultiUsage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const std::function<const TParentsMap*()>& getParents, TTypeAnnotationContext& typeAnnCtx) {
    auto providerRead = node.Cast<TDqReadWrap>().Input();
    if (auto dqOpt = GetDqOptCallback(providerRead, typeAnnCtx)) {
        TNodeOnNodeOwnedMap toOptimize;
        TExprNode::TPtr res;
        bool error = false;
        OptimizeSubsetFieldsForNodeWithMultiUsage(node.Ptr(), *getParents(), toOptimize, ctx,
            [&] (const TExprNode::TPtr& input, const TExprNode::TPtr& members, const TParentsMap&, TExprContext& ctx) -> TExprNode::TPtr {
                auto updatedRead = dqOpt->ApplyExtractMembers(providerRead.Ptr(), members, ctx);
                if (!updatedRead) {
                    error = true;
                    return {};
                }
                if (updatedRead != providerRead.Ptr()) {
                    res = ctx.ChangeChild(node.Ref(), TDqReadWrap::idx_Input, std::move(updatedRead));
                    return res;
                }

                return input;
            }
        );
        if (error) {
            return {};
        }
        if (!toOptimize.empty()) {
            for (auto& [s, d]: toOptimize) {
                optCtx.RemapNode(*s, d);
            }
            return TExprBase(res);
        }
    }

    return node;
}

TMaybeNode<TExprBase> UnorderedOverDqReadWrapMultiUsage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const std::function<const TParentsMap*()>& getParents, TTypeAnnotationContext& typeAnnCtx) {
    auto providerRead = node.Cast<TDqReadWrapBase>().Input();
    if (auto dqOpt = GetDqOptCallback(providerRead, typeAnnCtx)) {
        auto parentsMap = getParents();
        auto it = parentsMap->find(node.Raw());
        if (it == parentsMap->cend() || it->second.size() <= 1) {
            return node;
        }

        bool hasUnordered = false;
        for (auto parent: it->second) {
            if (TCoUnorderedBase::Match(parent)) {
                hasUnordered = true;
            } else if (!TCoAggregateBase::Match(parent) && !TCoFlatMap::Match(parent)) {
                return node;
            }
        }

        if (!hasUnordered) {
            return node;
        }

        auto updatedRead = dqOpt->ApplyUnordered(providerRead.Ptr(), ctx);
        if (!updatedRead) {
            return {};
        }
        if (updatedRead != providerRead.Ptr()) {
            auto newDqReadWrap = ctx.ChangeChild(node.Ref(), TDqReadWrapBase::idx_Input, std::move(updatedRead));
            for (auto parent: it->second) {
                if (TCoUnorderedBase::Match(parent)) {
                    optCtx.RemapNode(*parent, newDqReadWrap);
                } else if (TCoAggregateBase::Match(parent) || TCoFlatMap::Match(parent)) {
                    optCtx.RemapNode(*parent, ctx.ChangeChild(*parent, 0, TExprNode::TPtr(newDqReadWrap)));
                }
            }

            return TExprBase(newDqReadWrap);
        }
    }
    return node;
}

}

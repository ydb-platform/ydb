#include "logical_optimize.h"
#include "dqs_opt.h"

#include <ydb/library/yql/core/yql_aggregate_expander.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/dq/integration/yql_dq_optimization.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/cbo/cbo_optimizer.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/optimizer.h>

#include <util/generic/bitmap.h>

namespace NYql::NDqs {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {


TString BuildColumnName(const TExprBase column) {
    if (const auto columnName = column.Maybe<TCoAtom>()) {
        return columnName.Cast().StringValue();
    }

    if (const auto columnNames = column.Maybe<TCoAtomList>()) {
        TStringBuilder columnNameBuilder;
        for (const auto columnName : columnNames.Cast()) {
            columnNameBuilder.append(columnName.StringValue());
            columnNameBuilder.append("_");
        }
        return columnNameBuilder;
    }

    YQL_ENSURE(false, "Invalid node. Expected Atom or AtomList, but received: "
        << column.Ptr()->Dump());
}

bool IsLegacyHopping(const TExprNode::TPtr& hoppingSetting) {
    return !hoppingSetting->Child(1)->IsList();
}

}

class TDqsLogicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TDqsLogicalOptProposalTransformer(TTypeAnnotationContext* typeCtx, const TDqConfiguration::TPtr& config)
        : TOptimizeTransformerBase(/*TODO*/nullptr, NLog::EComponent::ProviderDq, {})
        , Config(config)
        , TypesCtx(*typeCtx)
    {
#define HNDL(name) "DqsLogical-"#name, Hndl(&TDqsLogicalOptProposalTransformer::name)
        AddHandler(0, &TCoUnorderedBase::Match, HNDL(SkipUnordered));
        AddHandler(0, &TCoUnorderedBase::Match, HNDL(UnorderedOverDqReadWrap));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqReadWrap));
        AddHandler(0, &TCoCountBase::Match, HNDL(TakeOrSkipOverDqReadWrap));
        AddHandler(0, &TCoExtendBase::Match, HNDL(ExtendOverDqReadWrap));
        AddHandler(0, &TCoNarrowMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoNarrowFlatMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoNarrowMultiMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoWideMap::Match, HNDL(DqReadWideWrapFieldSubset));
        AddHandler(0, &TCoAggregateBase::Match, HNDL(RewriteAggregate));
        AddHandler(0, &TCoTake::Match, HNDL(RewriteTakeSortToTopSort));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(OptimizeEquiJoinWithCosts));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(RewriteEquiJoin));
        AddHandler(0, &TCoCalcOverWindowBase::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoCalcOverWindowGroup::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoMatchRecognize::Match, HNDL(ExpandMatchRecognize));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(FlatMapOverExtend));
        AddHandler(0, &TDqQuery::Match, HNDL(MergeQueriesWithSinks));
        AddHandler(0, &TCoSqlIn::Match, HNDL(SqlInDropCompact));
        AddHandler(0, &TDqReplicate::Match, HNDL(ReplicateFieldSubset));

        AddHandler(1, &TDqReadWrapBase::Match, HNDL(DqReadWrapByProvider));

        AddHandler(2, &TDqReadWrap::Match, HNDL(ExtractMembersOverDqReadWrapMultiUsage));
        AddHandler(2, &TDqReadWrapBase::Match, HNDL(UnorderedOverDqReadWrapMultiUsage));
#undef HNDL

        SetGlobal(2u);
    }

protected:
    TMaybeNode<TExprBase> SkipUnordered(TExprBase node, TExprContext& ctx) {
        Y_UNUSED(ctx);
        const auto unordered = node.Cast<TCoUnorderedBase>();
        if (unordered.Input().Maybe<TDqConnection>()) {
            return unordered.Input();
        }
        return node;
    }

    TMaybeNode<TExprBase> UnorderedOverDqReadWrap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        const auto unordered = node.Cast<TCoUnorderedBase>();
        if (const auto maybeRead = unordered.Input().Maybe<TDqReadWrapBase>().Input()) {
            if (Config->EnableDqReplicate.Get().GetOrElse(TDqSettings::TDefault::EnableDqReplicate)) {
                const TParentsMap* parentsMap = getParents();
                auto parentsIt = parentsMap->find(unordered.Input().Raw());
                YQL_ENSURE(parentsIt != parentsMap->cend());
                if (parentsIt->second.size() > 1) {
                    return node;
                }
            }
            auto providerRead = maybeRead.Cast();
            if (auto dqOpt = GetDqOptCallback(providerRead)) {
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

    TMaybeNode<TExprBase> ExtractMembersOverDqReadWrap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        auto extract = node.Cast<TCoExtractMembers>();
        if (const auto maybeRead = extract.Input().Maybe<TDqReadWrap>().Input()) {
            if (Config->EnableDqReplicate.Get().GetOrElse(TDqSettings::TDefault::EnableDqReplicate)) {
                const TParentsMap* parentsMap = getParents();
                auto parentsIt = parentsMap->find(extract.Input().Raw());
                YQL_ENSURE(parentsIt != parentsMap->cend());
                if (parentsIt->second.size() > 1) {
                    return node;
                }
            }
            auto providerRead = maybeRead.Cast();
            if (auto dqOpt = GetDqOptCallback(providerRead)) {
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

    TMaybeNode<TExprBase> TakeOrSkipOverDqReadWrap(TExprBase node, TExprContext& ctx) {
        auto countBase = node.Cast<TCoCountBase>();

        // TODO: support via precomputes
        if (!TCoIntegralCtor::Match(countBase.Count().Raw())) {
            return node;
        }

        if (const auto maybeRead = countBase.Input().Maybe<TDqReadWrapBase>().Input()) {
            auto providerRead = maybeRead.Cast();
            if (auto dqOpt = GetDqOptCallback(providerRead)) {
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

    TMaybeNode<TExprBase> ExtendOverDqReadWrap(TExprBase node, TExprContext& ctx) const {
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
            IDqOptimization* dqOpt = GetDqOptCallback(dqReadWrap.Input());
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

    TMaybeNode<TExprBase> DqReadWideWrapFieldSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
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
            if (auto dqOpt = GetDqOptCallback(providerRead)) {

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

    TMaybeNode<TExprBase> FlatMapOverExtend(TExprBase node, TExprContext& ctx) {
        return DqFlatMapOverExtend(node, ctx);
    }

    TMaybeNode<TExprBase> RewriteAggregate(TExprBase node, TExprContext& ctx) {
        if (!Config->UseFinalizeByKey.Get().GetOrElse(false) && node.Maybe<TCoAggregate>()) {
            auto aggregate = node.Cast<TCoAggregate>();
            auto input = aggregate.Input().Maybe<TDqConnection>();

            if (input) {
                auto newNode = TAggregateExpander::CountAggregateRewrite(aggregate, ctx, TypesCtx.UseBlocks);
                if (node.Ptr() != newNode) {
                    return TExprBase(newNode);
                }
            }
        }
        auto aggregate = node.Cast<TCoAggregateBase>();
        auto input = aggregate.Input().Maybe<TDqConnection>();

        auto hopSetting = GetSetting(aggregate.Settings().Ref(), "hopping");
        if (input) {
            if (hopSetting) {
                return RewriteAsHoppingWindow(node, ctx, input.Cast());
            } else {
                return DqRewriteAggregate(node, ctx, TypesCtx, true, Config->UseAggPhases.Get().GetOrElse(false), Config->UseFinalizeByKey.Get().GetOrElse(false));
            }
        }
        return node;
    }

    TMaybeNode<TExprBase> RewriteTakeSortToTopSort(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        if (node.Maybe<TCoTake>().Input().Maybe<TCoSort>().Input().Maybe<TDqConnection>()) {
            return DqRewriteTakeSortToTopSort(node, ctx, *getParents());
        }
        return node;
    }

    TMaybeNode<TExprBase> OptimizeEquiJoinWithCosts(TExprBase node, TExprContext& ctx) {
        if (TypesCtx.CostBasedOptimizer != ECostBasedOptimizerType::Disable) {
            std::function<void(const TString&)> log = [&](auto str) {
                YQL_CLOG(INFO, ProviderDq) << str;
            };
            std::function<IOptimizer*(IOptimizer::TInput&&)> factory = [&](auto input) {
                if (TypesCtx.CostBasedOptimizer == ECostBasedOptimizerType::Native) {
                    return MakeNativeOptimizer(input, log);
                } else if (TypesCtx.CostBasedOptimizer == ECostBasedOptimizerType::PG) {
                    return MakePgOptimizer(input, log);
                } else {
                    YQL_ENSURE(false, "Unknown CBO type");
                }
            };
            return DqOptimizeEquiJoinWithCosts(node, ctx, TypesCtx, factory, true);
        } else {
            return node;
        }
    }

    TMaybeNode<TExprBase> RewriteEquiJoin(TExprBase node, TExprContext& ctx) {
        auto equiJoin = node.Cast<TCoEquiJoin>();
        bool hasDqConnections = false;
        for (size_t i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
            auto list = equiJoin.Arg(i).Cast<TCoEquiJoinInput>().List();
            if (auto maybeExtractMembers = list.Maybe<TCoExtractMembers>()) {
                list = maybeExtractMembers.Cast().Input();
            }
            if (auto maybeFlatMap = list.Maybe<TCoFlatMapBase>()) {
                list = maybeFlatMap.Cast().Input();
            }
            hasDqConnections |= !!list.Maybe<TDqConnection>();
        }

        return hasDqConnections ? DqRewriteEquiJoin(node, Config->HashJoinMode.Get().GetOrElse(EHashJoinMode::Off), ctx) : node;
    }

    TMaybeNode<TExprBase> ExpandWindowFunctions(TExprBase node, TExprContext& ctx) {
        if (node.Cast<TCoInputBase>().Input().Maybe<TDqConnection>()) {
            return DqExpandWindowFunctions(node, ctx, true);
        }
        return node;
    }

    TMaybeNode<TExprBase> ExpandMatchRecognize(TExprBase node, TExprContext& ctx) {
        if (node.Maybe<TCoMatchRecognize>() &&
            node.Cast<TCoInputBase>().Input().Maybe<TDqConnection>()
        ) {
            return DqExpandMatchRecognize(node, ctx, TypesCtx);
        }
        return node;
    }

    TMaybeNode<TExprBase> MergeQueriesWithSinks(TExprBase node, TExprContext& ctx) {
        return DqMergeQueriesWithSinks(node, ctx);
    }

    TMaybeNode<TExprBase> SqlInDropCompact(TExprBase node, TExprContext& ctx) const {
        return DqSqlInDropCompact(node, ctx);
    }

    TMaybeNode<TExprBase> ReplicateFieldSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        return DqReplicateFieldSubset(node, ctx, *getParents());
    }

    TMaybeNode<TExprBase> DqReadWrapByProvider(TExprBase node, TExprContext& ctx) const {
        auto providerRead = node.Cast<TDqReadWrapBase>().Input();
        if (auto dqOpt = GetDqOptCallback(providerRead)) {
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

    TMaybeNode<TExprBase> ExtractMembersOverDqReadWrapMultiUsage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        auto providerRead = node.Cast<TDqReadWrap>().Input();
        if (auto dqOpt = GetDqOptCallback(providerRead)) {
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

    TMaybeNode<TExprBase> UnorderedOverDqReadWrapMultiUsage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        auto providerRead = node.Cast<TDqReadWrapBase>().Input();
        if (auto dqOpt = GetDqOptCallback(providerRead)) {
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

private:
    TMaybeNode<TExprBase> RewriteAsHoppingWindow(const TExprBase node, TExprContext& ctx, const TDqConnection& input) {
        const auto aggregate = node.Cast<TCoAggregate>();
        const auto pos = aggregate.Pos();

        YQL_CLOG(DEBUG, ProviderDq) << "OptimizeStreamingAggregate";

        EnsureNotDistinct(aggregate);

        const auto maybeHopTraits = ExtractHopTraits(aggregate, ctx);
        if (!maybeHopTraits) {
            return nullptr;
        }
        const auto hopTraits = *maybeHopTraits;

        const auto aggregateInputType = GetSeqItemType(*node.Ptr()->Head().GetTypeAnn()).Cast<TStructExprType>();
        TKeysDescription keysDescription(*aggregateInputType, aggregate.Keys(), hopTraits.Column);

        if (keysDescription.NeedPickle()) {
            return Build<TCoMap>(ctx, pos)
                .Lambda(keysDescription.BuildUnpickleLambda(ctx, pos, *aggregateInputType))
                .Input<TCoAggregate>()
                    .InitFrom(aggregate)
                    .Input<TCoMap>()
                        .Lambda(keysDescription.BuildPickleLambda(ctx, pos))
                        .Input(input)
                    .Build()
                .Build()
                .Done();
        }

        const auto keyLambda = keysDescription.GetKeySelector(ctx, pos, aggregateInputType);
        const auto timeExtractorLambda = BuildTimeExtractor(hopTraits.Traits, ctx);
        const auto initLambda = BuildInitHopLambda(aggregate, ctx);
        const auto updateLambda = BuildUpdateHopLambda(aggregate, ctx);
        const auto saveLambda = BuildSaveHopLambda(aggregate, ctx);
        const auto loadLambda = BuildLoadHopLambda(aggregate, ctx);
        const auto mergeLambda = BuildMergeHopLambda(aggregate, ctx);
        const auto finishLambda = BuildFinishHopLambda(aggregate, keysDescription.GetActualGroupKeys(), hopTraits.Column, ctx);
        const auto enableWatermarks = BuildWatermarkMode(aggregate, hopTraits.Traits, ctx);
        if (!enableWatermarks) {
            return nullptr;
        }

        const auto streamArg = Build<TCoArgument>(ctx, pos).Name("stream").Done();
        auto multiHoppingCoreBuilder = Build<TCoMultiHoppingCore>(ctx, pos)
            .KeyExtractor(keyLambda)
            .TimeExtractor(timeExtractorLambda)
            .Hop(hopTraits.Traits.Hop())
            .Interval(hopTraits.Traits.Interval())
            .DataWatermarks(hopTraits.Traits.DataWatermarks())
            .InitHandler(initLambda)
            .UpdateHandler(updateLambda)
            .MergeHandler(mergeLambda)
            .FinishHandler(finishLambda)
            .SaveHandler(saveLambda)
            .LoadHandler(loadLambda)
            .WatermarkMode<TCoAtom>().Build(ToString(*enableWatermarks));

        if (*enableWatermarks) {
            const auto hop = TDuration::MicroSeconds(hopTraits.Hop);
            const auto lateArrivalDelay = TDuration::MilliSeconds(Config->WatermarksLateArrivalDelayMs
                .Get()
                .GetOrElse(TDqSettings::TDefault::WatermarksLateArrivalDelayMs));

            multiHoppingCoreBuilder.Delay<TCoInterval>()
                .Literal().Build(ToString(Max(hop, lateArrivalDelay).MicroSeconds()))
                .Build();
        } else {
            multiHoppingCoreBuilder.Delay(hopTraits.Traits.Delay());
        }

        if (Config->AnalyticsHopping.Get().GetOrElse(false)) {
            return Build<TCoPartitionsByKeys>(ctx, node.Pos())
                .Input(input.Ptr())
                .KeySelectorLambda(keyLambda)
                .SortDirections<TCoBool>()
                        .Literal()
                        .Value("true")
                        .Build()
                    .Build()
                .SortKeySelectorLambda(timeExtractorLambda)
                .ListHandlerLambda()
                    .Args(streamArg)
                    .Body<TCoForwardList>()
                        .Stream(multiHoppingCoreBuilder
                            .Input<TCoIterator>()
                                .List(streamArg)
                                .Build()
                            .Done())
                        .Build()
                    .Build()
                .Done();
        } else {
            auto wrappedInput = input.Ptr();
            if (!keysDescription.MemberKeys.empty()) {
                // Shuffle input connection by keys
                wrappedInput = WrapToShuffle(keysDescription, aggregate, input, ctx);
                if (!wrappedInput) {
                    return nullptr;
                }
            }

            const auto stage = Build<TDqStage>(ctx, node.Pos())
                .Inputs()
                    .Add(wrappedInput)
                    .Build()
                .Program()
                    .Args(streamArg)
                    .Body<TCoMap>()
                        .Input(multiHoppingCoreBuilder
                            .Input<TCoFromFlow>()
                                .Input(streamArg)
                                .Build()
                            .Done())
                        .Lambda(keysDescription.BuildUnpickleLambda(ctx, pos, *aggregateInputType))
                        .Build()
                    .Build()
                .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
                .Done();

            return Build<TDqCnUnionAll>(ctx, node.Pos())
                .Output()
                    .Stage(stage)
                    .Index().Build(0)
                    .Build()
                .Done();
        }
    }

    struct THoppingTraits {
        TString Column;
        TCoHoppingTraits Traits;
        ui64 Hop;
        ui64 Interval;
        ui64 Delay;
    };

    TMaybe<THoppingTraits> ExtractHopTraits(const TCoAggregate& aggregate, TExprContext& ctx) {
        const auto pos = aggregate.Pos();

        const auto hopSetting = GetSetting(aggregate.Settings().Ref(), "hopping");
        if (!hopSetting) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Aggregate over stream must have 'hopping' setting"));
            return Nothing();
        }

        const auto hoppingColumn = IsLegacyHopping(hopSetting)
            ? "_yql_time"
            : TString(hopSetting->Child(1)->Child(0)->Content());

        const auto traitsNode = IsLegacyHopping(hopSetting)
            ? hopSetting->Child(1)
            : hopSetting->Child(1)->Child(1);

        const auto maybeTraits = TMaybeNode<TCoHoppingTraits>(traitsNode);
        if (!maybeTraits) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Invalid 'hopping' setting in Aggregate"));
            return Nothing();
        }

        const auto traits = maybeTraits.Cast();

        const auto checkIntervalParam = [&] (TExprBase param) -> ui64 {
            if (param.Maybe<TCoJust>()) {
                param = param.Cast<TCoJust>().Input();
            }
            if (!param.Maybe<TCoInterval>()) {
                ctx.AddError(TIssue(ctx.GetPosition(pos), "Not an interval data ctor"));
                return 0;
            }
            auto value = FromString<i64>(param.Cast<TCoInterval>().Literal().Value());
            if (value <= 0) {
                ctx.AddError(TIssue(ctx.GetPosition(pos), "Interval value must be positive"));
                return 0;
            }
            return (ui64)value;
        };

        const auto hop = checkIntervalParam(traits.Hop());
        if (!hop) {
            return Nothing();
        }
        const auto interval = checkIntervalParam(traits.Interval());
        if (!interval) {
            return Nothing();
        }
        const auto delay = checkIntervalParam(traits.Delay());
        if (!delay) {
            return Nothing();
        }

        if (interval < hop) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Interval must be greater or equal then hop"));
            return Nothing();
        }
        if (delay < hop) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Delay must be greater or equal then hop"));
            return Nothing();
        }

        const auto newTraits = Build<TCoHoppingTraits>(ctx, aggregate.Pos())
            .InitFrom(traits)
            .DataWatermarks(Config->AnalyticsHopping.Get().GetOrElse(false)
                ? ctx.NewAtom(aggregate.Pos(), "false")
                : traits.DataWatermarks().Ptr())
            .Done();

        return THoppingTraits {
            hoppingColumn,
            newTraits,
            hop,
            interval,
            delay
        };
    }

    struct TKeysDescription {
        TVector<TString> PickleKeys;
        TVector<TString> MemberKeys;
        TVector<TString> FakeKeys;

        explicit TKeysDescription(const TStructExprType& rowType, const TCoAtomList& keys, const TString& hoppingColumn) {
            for (const auto& key : keys) {
                if (key.StringValue() == hoppingColumn) {
                    FakeKeys.emplace_back(key.StringValue());
                    continue;
                }

                const auto index = rowType.FindItem(key.StringValue());
                Y_ENSURE(index);

                auto itemType = rowType.GetItems()[*index]->GetItemType();
                if (RemoveOptionalType(itemType)->GetKind() == ETypeAnnotationKind::Data) {
                    MemberKeys.emplace_back(key.StringValue());
                    continue;
                }

                PickleKeys.emplace_back(key.StringValue());
            }
        }

        TExprNode::TPtr BuildPickleLambda(TExprContext& ctx, TPositionHandle pos) const {
            TCoArgument arg = Build<TCoArgument>(ctx, pos)
                .Name("item")
                .Done();

            TExprBase body = arg;

            for (const auto& key : PickleKeys) {
                const auto member = Build<TCoMember>(ctx, pos)
                        .Name().Build(key)
                        .Struct(arg)
                    .Done()
                    .Ptr();

                body = Build<TCoReplaceMember>(ctx, pos)
                    .Struct(body)
                    .Name().Build(key)
                    .Item(ctx.NewCallable(pos, "StablePickle", { member }))
                    .Done();
            }

            return Build<TCoLambda>(ctx, pos)
                .Args({arg})
                .Body(body)
                .Done()
                .Ptr();
        }

        TExprNode::TPtr BuildUnpickleLambda(TExprContext& ctx, TPositionHandle pos, const TStructExprType& rowType) {
            TCoArgument arg = Build<TCoArgument>(ctx, pos)
                .Name("item")
                .Done();

            TExprBase body = arg;

            for (const auto& key : PickleKeys) {
                const auto index = rowType.FindItem(key);
                Y_ENSURE(index);

                auto itemType = rowType.GetItems().at(*index)->GetItemType();
                const auto member = Build<TCoMember>(ctx, pos)
                        .Name().Build(key)
                        .Struct(arg)
                    .Done()
                    .Ptr();

                body = Build<TCoReplaceMember>(ctx, pos)
                    .Struct(body)
                    .Name().Build(key)
                    .Item(ctx.NewCallable(pos, "Unpickle", { ExpandType(pos, *itemType, ctx), member }))
                    .Done();
            }

            return Build<TCoLambda>(ctx, pos)
                .Args({arg})
                .Body(body)
                .Done()
                .Ptr();
        }

        TVector<TCoAtom> GetKeysList(TExprContext& ctx, TPositionHandle pos) const {
            TVector<TCoAtom> res;
            res.reserve(PickleKeys.size() + MemberKeys.size());

            for (const auto& pickleKey : PickleKeys) {
                res.emplace_back(Build<TCoAtom>(ctx, pos).Value(pickleKey).Done());
            }
            for (const auto& memberKey : MemberKeys) {
                res.emplace_back(Build<TCoAtom>(ctx, pos).Value(memberKey).Done());
            }
            return res;
        }

        TVector<TString> GetActualGroupKeys() {
            TVector<TString> result;
            result.reserve(PickleKeys.size() + MemberKeys.size());
            result.insert(result.end(), PickleKeys.begin(), PickleKeys.end());
            result.insert(result.end(), MemberKeys.begin(), MemberKeys.end());
            return result;
        }

        bool NeedPickle() const {
            return !PickleKeys.empty();
        }

        TExprNode::TPtr GetKeySelector(TExprContext& ctx, TPositionHandle pos, const TStructExprType* rowType) {
            auto builder = Build<TCoAtomList>(ctx, pos);
            for (auto key : GetKeysList(ctx, pos)) {
                builder.Add(std::move(key));
            }
            return BuildKeySelector(pos, *rowType, builder.Build().Value().Ptr(), ctx);
        }
    };

    TExprNode::TPtr WrapToShuffle(
        const TKeysDescription& keysDescription,
        const TCoAggregate& aggregate,
        const TDqConnection& input,
        TExprContext& ctx)
    {
        auto pos = aggregate.Pos();

        TDqStageBase mappedInput = input.Output().Stage();
        if (keysDescription.NeedPickle()) {
            mappedInput = Build<TDqStage>(ctx, pos)
                .Inputs()
                    .Add<TDqCnMap>()
                        .Output()
                            .Stage(input.Output().Stage())
                            .Index(input.Output().Index())
                            .Build()
                        .Build()
                    .Build()
                .Program()
                    .Args({"stream"})
                    .Body<TCoMap>()
                        .Input("stream")
                        .Lambda(keysDescription.BuildPickleLambda(ctx, pos))
                    .Build()
                .Build()
                .Settings(TDqStageSettings().BuildNode(ctx, pos))
                .Done();
        }

        return Build<TDqCnHashShuffle>(ctx, pos)
            .Output()
                .Stage(mappedInput)
                .Index().Value("0").Build()
                .Build()
            .KeyColumns()
                .Add(keysDescription.GetKeysList(ctx, pos))
                .Build()
            .Done()
            .Ptr();
    }

    void EnsureNotDistinct(const TCoAggregate& aggregate) {
        const auto& aggregateHandlers = aggregate.Handlers();

        YQL_ENSURE(
            AllOf(aggregateHandlers, [](const auto& t){ return !t.DistinctName(); }),
            "Distinct is not supported for aggregation with hop");
    }

    TExprNode::TPtr BuildTimeExtractor(const TCoHoppingTraits& hoppingTraits, TExprContext& ctx) {
        const auto pos = hoppingTraits.Pos();

        if (hoppingTraits.ItemType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>()->GetSize() == 0) {
            // The case when no fields are used in lambda. F.e. when it has only DependsOn.
            return ctx.DeepCopyLambda(hoppingTraits.TimeExtractor().Ref());
        }

        return Build<TCoLambda>(ctx, pos)
            .Args({"item"})
            .Body<TExprApplier>()
                .Apply(hoppingTraits.TimeExtractor())
                .With<TCoSafeCast>(0)
                    .Type(hoppingTraits.ItemType())
                    .Value("item")
                    .Build()
                .Build()
            .Done()
            .Ptr();
    }

    TExprNode::TPtr BuildInitHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
        const auto pos = aggregate.Pos();
        const auto& aggregateHandlers = aggregate.Handlers();

        const auto initItemArg = Build<TCoArgument>(ctx, pos).Name("item").Done();

        TVector<TExprBase> structItems;
        structItems.reserve(aggregateHandlers.Size());

        ui32 index = 0;
        for (const auto& handler : aggregateHandlers) {
            const auto tuple = handler.Cast<TCoAggregateTuple>();

            TMaybeNode<TExprBase> applier;
            if (tuple.Trait().Cast<TCoAggregationTraits>().InitHandler().Args().Size() == 1) {
                applier = Build<TExprApplier>(ctx, pos)
                    .Apply(tuple.Trait().Cast<TCoAggregationTraits>().InitHandler())
                    .With(0, initItemArg)
                    .Done();
            } else {
                applier = Build<TExprApplier>(ctx, pos)
                    .Apply(tuple.Trait().Cast<TCoAggregationTraits>().InitHandler())
                    .With(0, initItemArg)
                    .With<TCoUint32>(1)
                        .Literal().Build(ToString(index))
                        .Build()
                    .Done();
            }

            structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(BuildColumnName(tuple.ColumnName()))
                .Value(applier)
                .Done());
            ++index;
        }

        return Build<TCoLambda>(ctx, pos)
            .Args({initItemArg})
            .Body<TCoAsStruct>()
                .Add(structItems)
                .Build()
            .Done()
            .Ptr();
    }

    TExprNode::TPtr BuildUpdateHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
        const auto pos = aggregate.Pos();
        const auto aggregateHandlers = aggregate.Handlers();

        const auto updateItemArg = Build<TCoArgument>(ctx, pos).Name("item").Done();
        const auto updateStateArg = Build<TCoArgument>(ctx, pos).Name("state").Done();

        TVector<TExprBase> structItems;
        structItems.reserve(aggregateHandlers.Size());

        i32 index = 0;
        for (const auto& handler : aggregateHandlers) {
            const auto tuple = handler.Cast<TCoAggregateTuple>();
            const TString columnName = BuildColumnName(tuple.ColumnName());

            const auto member = Build<TCoMember>(ctx, pos)
                .Struct(updateStateArg)
                .Name().Build(columnName)
                .Done();

            TMaybeNode<TExprBase> applier;
            if (tuple.Trait().Cast<TCoAggregationTraits>().UpdateHandler().Args().Size() == 2) {
                applier = Build<TExprApplier>(ctx, pos)
                    .Apply(tuple.Trait().Cast<TCoAggregationTraits>().UpdateHandler())
                    .With(0, updateItemArg)
                    .With(1, member)
                    .Done();
            } else {
                applier = Build<TExprApplier>(ctx, pos)
                    .Apply(tuple.Trait().Cast<TCoAggregationTraits>().UpdateHandler())
                    .With(0, updateItemArg)
                    .With(1, member)
                    .With<TCoUint32>(2)
                        .Literal().Build(ToString(index))
                        .Build()
                    .Done();
            }

            structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(columnName)
                .Value(applier)
                .Done());
            ++index;
        }

        return Build<TCoLambda>(ctx, pos)
            .Args({updateItemArg, updateStateArg})
            .Body<TCoAsStruct>()
                .Add(structItems)
                .Build()
            .Done()
            .Ptr();
    }

    TExprNode::TPtr BuildMergeHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
        const auto pos = aggregate.Pos();
        const auto& aggregateHandlers = aggregate.Handlers();

        const auto mergeState1Arg = Build<TCoArgument>(ctx, pos).Name("state1").Done();
        const auto mergeState2Arg = Build<TCoArgument>(ctx, pos).Name("state2").Done();

        TVector<TExprBase> structItems;
        structItems.reserve(aggregateHandlers.Size());

        for (const auto& handler : aggregateHandlers) {
            const auto tuple = handler.Cast<TCoAggregateTuple>();
            const TString columnName = BuildColumnName(tuple.ColumnName());

            const auto member1 = Build<TCoMember>(ctx, pos)
                .Struct(mergeState1Arg)
                .Name().Build(columnName)
                .Done();
            const auto member2 = Build<TCoMember>(ctx, pos)
                .Struct(mergeState2Arg)
                .Name().Build(columnName)
                .Done();

            structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(columnName)
                .Value<TExprApplier>()
                    .Apply(tuple.Trait().Cast<TCoAggregationTraits>().MergeHandler())
                    .With(0, member1)
                    .With(1, member2)
                    .Build()
                .Done());
        }

        return Build<TCoLambda>(ctx, pos)
            .Args({mergeState1Arg, mergeState2Arg})
            .Body<TCoAsStruct>()
                .Add(structItems)
                .Build()
            .Done()
            .Ptr();
    }

    TExprNode::TPtr BuildFinishHopLambda(
        const TCoAggregate& aggregate,
        const TVector<TString>& actualGroupKeys,
        const TString& hoppingColumn,
        TExprContext& ctx)
    {
        const auto pos = aggregate.Pos();
        const auto aggregateHandlers = aggregate.Handlers();

        const auto finishKeyArg = Build<TCoArgument>(ctx, pos).Name("key").Done();
        const auto finishStateArg = Build<TCoArgument>(ctx, pos).Name("state").Done();
        const auto finishTimeArg = Build<TCoArgument>(ctx, pos).Name("time").Done();

        TVector<TExprBase> structItems;
        structItems.reserve(actualGroupKeys.size() + aggregateHandlers.Size() + 1);

        if (actualGroupKeys.size() == 1) {
            structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(actualGroupKeys[0])
                .Value(finishKeyArg)
                .Done());
        } else {
            for (size_t i = 0; i < actualGroupKeys.size(); ++i) {
                structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                    .Name().Build(actualGroupKeys[i])
                    .Value<TCoNth>()
                        .Tuple(finishKeyArg)
                        .Index<TCoAtom>()
                            .Value(ToString(i))
                            .Build()
                        .Build()
                    .Done());
            }
        }

        for (const auto& handler : aggregateHandlers) {
            const auto tuple = handler.Cast<TCoAggregateTuple>();
            const TString compoundColumnName = BuildColumnName(tuple.ColumnName());

            const auto member = Build<TCoMember>(ctx, pos)
                .Struct(finishStateArg)
                .Name().Build(compoundColumnName)
                .Done();

            if (tuple.ColumnName().Maybe<TCoAtom>()) {
                structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                    .Name().Build(compoundColumnName)
                    .Value<TExprApplier>()
                        .Apply(tuple.Trait().Cast<TCoAggregationTraits>().FinishHandler())
                        .With(0, member)
                        .Build()
                    .Done());

                continue;
            }

            if (const auto namesList = tuple.ColumnName().Maybe<TCoAtomList>()) {
                const auto expApplier = Build<TExprApplier>(ctx, pos)
                    .Apply(tuple.Trait().Cast<TCoAggregationTraits>().FinishHandler())
                    .With(0, member)
                    .Done();

                int index = 0;
                for (const auto columnName : namesList.Cast()) {
                    const auto extracter = Build<TCoNth>(ctx, pos)
                        .Tuple(expApplier)
                        .Index<TCoAtom>().Build(index++)
                        .Done();

                    structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                        .Name(columnName)
                        .Value(extracter)
                        .Done());
                }

                continue;
            }

            YQL_ENSURE(false, "Invalid node. Expected Atom or AtomList, but received: "
                << tuple.ColumnName().Ptr()->Dump());
        }

        structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(hoppingColumn)
            .Value(finishTimeArg)
            .Done());

        return Build<TCoLambda>(ctx, pos)
            .Args({finishKeyArg, finishStateArg, finishTimeArg})
            .Body<TCoAsStruct>()
                .Add(structItems)
                .Build()
            .Done()
            .Ptr();
    }

    TExprNode::TPtr BuildSaveHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
        const auto pos = aggregate.Pos();
        const auto aggregateHandlers = aggregate.Handlers();

        const auto saveStateArg = Build<TCoArgument>(ctx, pos).Name("state").Done();

        TVector<TExprBase> structItems;
        structItems.reserve(aggregateHandlers.Size());

        for (const auto& handler : aggregateHandlers) {
            const auto tuple = handler.Cast<TCoAggregateTuple>();
            const TString columnName = BuildColumnName(tuple.ColumnName());

            const auto member = Build<TCoMember>(ctx, pos)
                .Struct(saveStateArg)
                .Name().Build(columnName)
                .Done();

            structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(columnName)
                .Value<TExprApplier>()
                    .Apply(tuple.Trait().Cast<TCoAggregationTraits>().SaveHandler())
                    .With(0, member)
                    .Build()
                .Done());
        }

        return Build<TCoLambda>(ctx, pos)
            .Args({saveStateArg})
            .Body<TCoAsStruct>()
                .Add(structItems)
                .Build()
            .Done()
            .Ptr();
    }

    TExprNode::TPtr BuildLoadHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
        const auto pos = aggregate.Pos();
        const auto aggregateHandlers = aggregate.Handlers();

        TCoArgument loadStateArg = Build<TCoArgument>(ctx, pos).Name("state").Done();

        TVector<TExprBase> structItems;
        structItems.reserve(aggregateHandlers.Size());

        for (const auto& handler : aggregateHandlers) {
            const auto tuple = handler.Cast<TCoAggregateTuple>();
            const TString columnName = BuildColumnName(tuple.ColumnName());

            const auto member = Build<TCoMember>(ctx, pos)
                .Struct(loadStateArg)
                .Name().Build(columnName)
                .Done();

            structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(columnName)
                .Value<TExprApplier>()
                    .Apply(tuple.Trait().Cast<TCoAggregationTraits>().LoadHandler())
                    .With(0, member)
                    .Build()
                .Done());
        }

        return Build<TCoLambda>(ctx, pos)
            .Args({loadStateArg})
            .Body<TCoAsStruct>()
                .Add(structItems)
                .Build()
            .Done()
            .Ptr();
    }

    TMaybe<bool> BuildWatermarkMode(
        const TCoAggregate& aggregate,
        const TCoHoppingTraits& hoppingTraits,
        TExprContext& ctx)
    {
        const auto analyticsMode = Config->AnalyticsHopping.Get().GetOrElse(false);
        const bool enableWatermarks = !analyticsMode &&
            Config->WatermarksMode.Get() == "default" &&
            hoppingTraits.Version().Cast<TCoAtom>().StringValue() == "v2";
        if (enableWatermarks && Config->ComputeActorType.Get() != "async") {
            ctx.AddError(TIssue(ctx.GetPosition(aggregate.Pos()), "Watermarks should be used only with async compute actor"));
            return Nothing();
        }

        if (hoppingTraits.Version().Cast<TCoAtom>().StringValue() == "v2" && !enableWatermarks) {
            ctx.AddError(TIssue(
                ctx.GetPosition(aggregate.Pos()),
                "HoppingWindow requires watermarks to be enabled. If you don't want to do that, you can use HOP instead."));
            return Nothing();
        }

        return enableWatermarks;
    }

    IDqOptimization* GetDqOptCallback(const TExprBase& providerRead) const {
        if (providerRead.Ref().ChildrenSize() > 1 && TCoDataSource::Match(providerRead.Ref().Child(1))) {
            auto dataSourceName = providerRead.Ref().Child(1)->Child(0)->Content();
            auto datasource = TypesCtx.DataSourceMap.FindPtr(dataSourceName);
            YQL_ENSURE(datasource);
            return (*datasource)->GetDqOptimization();
        }
        return nullptr;
    }

private:
    TDqConfiguration::TPtr Config;
    TTypeAnnotationContext& TypesCtx;
};

THolder<IGraphTransformer> CreateDqsLogOptTransformer(TTypeAnnotationContext* typeCtx, const TDqConfiguration::TPtr& config) {
    return THolder(new TDqsLogicalOptProposalTransformer(typeCtx, config));
}

} // NYql::NDqs

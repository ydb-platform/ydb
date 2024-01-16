#include "logical_optimize.h"
#include "dqs_opt.h"

#include <ydb/library/yql/core/yql_aggregate_expander.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/dq/integration/yql_dq_optimization.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_hopping.h>
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
                auto newNode = TAggregateExpander::CountAggregateRewrite(aggregate, ctx, TypesCtx.IsBlockEngineEnabled());
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
                bool analyticsHopping = Config->AnalyticsHopping.Get().GetOrElse(false);
                const auto lateArrivalDelay = TDuration::MilliSeconds(Config->WatermarksLateArrivalDelayMs
                    .Get()
                    .GetOrElse(TDqSettings::TDefault::WatermarksLateArrivalDelayMs));
                bool defaultWatermarksMode = Config->WatermarksMode.Get() == "default";
                bool asyncActor = Config->ComputeActorType.Get() != "async";
                return NHopping::RewriteAsHoppingWindow(node, ctx, input.Cast(), analyticsHopping, lateArrivalDelay, defaultWatermarksMode, asyncActor);
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

    void EnsureNotDistinct(const TCoAggregate& aggregate) {
        const auto& aggregateHandlers = aggregate.Handlers();

        YQL_ENSURE(
            AllOf(aggregateHandlers, [](const auto& t){ return !t.DistinctName(); }),
            "Distinct is not supported for aggregation with hop");
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

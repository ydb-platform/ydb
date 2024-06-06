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
        return NDq::UnorderedOverDqReadWrap(node, ctx, getParents, Config->EnableDqReplicate.Get().GetOrElse(TDqSettings::TDefault::EnableDqReplicate), TypesCtx);
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqReadWrap(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        return NDq::ExtractMembersOverDqReadWrap(node, ctx, getParents, Config->EnableDqReplicate.Get().GetOrElse(TDqSettings::TDefault::EnableDqReplicate), TypesCtx);
    }

    TMaybeNode<TExprBase> TakeOrSkipOverDqReadWrap(TExprBase node, TExprContext& ctx) {
        return NDq::TakeOrSkipOverDqReadWrap(node, ctx, TypesCtx);
    }

    TMaybeNode<TExprBase> ExtendOverDqReadWrap(TExprBase node, TExprContext& ctx) const {
        return NDq::ExtendOverDqReadWrap(node, ctx, TypesCtx);
    }

    TMaybeNode<TExprBase> DqReadWideWrapFieldSubset(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        return NDq::DqReadWideWrapFieldSubset(node, ctx, getParents, TypesCtx);
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
                bool syncActor = Config->ComputeActorType.Get() != "async";
                return NHopping::RewriteAsHoppingWindow(node, ctx, input.Cast(), analyticsHopping, lateArrivalDelay, defaultWatermarksMode, syncActor);
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

            std::unique_ptr<IOptimizerNew> opt;
            TBaseProviderContext pctx;

            switch (TypesCtx.CostBasedOptimizer) {
            case ECostBasedOptimizerType::Native:
                opt = std::unique_ptr<IOptimizerNew>(NDq::MakeNativeOptimizerNew(pctx, 100000));
                break;
            case ECostBasedOptimizerType::PG:
                opt = std::unique_ptr<IOptimizerNew>(MakePgOptimizerNew(pctx, ctx, log));
                break;
            default:
                YQL_ENSURE(false, "Unknown CBO type");
                break;
            }
            std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)> providerCollect = [](auto& rels, auto label, auto node, auto stats) {
                Y_UNUSED(node);
                auto rel = std::make_shared<TRelOptimizerNode>(TString(label), stats);
                rels.push_back(rel);
            };

            return DqOptimizeEquiJoinWithCosts(node, ctx, TypesCtx, 1, *opt, providerCollect);
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

        return hasDqConnections ? DqRewriteEquiJoin(node, Config->HashJoinMode.Get().GetOrElse(EHashJoinMode::Off), false, ctx, TypesCtx) : node;
    }

    TMaybeNode<TExprBase> ExpandWindowFunctions(TExprBase node, TExprContext& ctx) {
        if (node.Cast<TCoInputBase>().Input().Maybe<TDqConnection>()) {
            return DqExpandWindowFunctions(node, ctx, TypesCtx, true);
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
        return NDq::DqReadWrapByProvider(node, ctx, TypesCtx);
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqReadWrapMultiUsage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return NDq::ExtractMembersOverDqReadWrapMultiUsage(node, ctx, optCtx, getParents, TypesCtx);
    }

    TMaybeNode<TExprBase> UnorderedOverDqReadWrapMultiUsage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) {
        return NDq::UnorderedOverDqReadWrapMultiUsage(node, ctx, optCtx, getParents, TypesCtx);
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

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

namespace {

bool IsStreamLookup(const TCoEquiJoinTuple& joinTuple) {
    for (const auto& outer: joinTuple.Options()) {
        for (const auto& inner: outer.Cast<TExprList>()) {
            if (inner.Cast<TCoAtom>().StringValue() == "forceStreamLookup") {
                return true;
            }
        }
    }
    return false;
}

}

/**
 * DQ Specific cost function and join applicability cost function
*/
struct TDqCBOProviderContext : public NYql::TBaseProviderContext {
    TDqCBOProviderContext(TTypeAnnotationContext& typeCtx, const TDqConfiguration::TPtr& config)
        : NYql::TBaseProviderContext()
        , Config(config)
        , TypesCtx(typeCtx) {}

    virtual bool IsJoinApplicable(const std::shared_ptr<NYql::IBaseOptimizerNode>& left,
        const std::shared_ptr<NYql::IBaseOptimizerNode>& right,
        const std::set<std::pair<NYql::NDq::TJoinColumn, NYql::NDq::TJoinColumn>>& joinConditions,
        const TVector<TString>& leftJoinKeys, const TVector<TString>& rightJoinKeys,
        NYql::EJoinAlgoType joinAlgo,  NYql::EJoinKind joinKind) override;

    virtual double ComputeJoinCost(const NYql::TOptimizerStatistics& leftStats, const NYql::TOptimizerStatistics& rightStats, const double outputRows, const double outputByteSize, NYql::EJoinAlgoType joinAlgo) const override;

    TDqConfiguration::TPtr Config;
    TTypeAnnotationContext& TypesCtx;
};


bool TDqCBOProviderContext::IsJoinApplicable(const std::shared_ptr<NYql::IBaseOptimizerNode>& left,
        const std::shared_ptr<NYql::IBaseOptimizerNode>& right,
        const std::set<std::pair<NYql::NDq::TJoinColumn, NYql::NDq::TJoinColumn>>& joinConditions,
        const TVector<TString>& leftJoinKeys, const TVector<TString>& rightJoinKeys,
        NYql::EJoinAlgoType joinAlgo,  NYql::EJoinKind joinKind) {
    Y_UNUSED(left);
    Y_UNUSED(right);
    Y_UNUSED(joinConditions);
    Y_UNUSED(leftJoinKeys);
    Y_UNUSED(rightJoinKeys);

    switch(joinAlgo) {

    case EJoinAlgoType::MapJoin:
        if (joinKind == EJoinKind::OuterJoin || joinKind == EJoinKind::Exclusion)
            return false;
        if (auto hashJoinMode = Config->HashJoinMode.Get().GetOrElse(EHashJoinMode::Off);
                hashJoinMode == EHashJoinMode::Off || hashJoinMode == EHashJoinMode::Map)
            return true;
        break;

    case EJoinAlgoType::GraceJoin:
        return true;

    default:
        break;
    }
    return false;
}


double TDqCBOProviderContext::ComputeJoinCost(const TOptimizerStatistics& leftStats, const TOptimizerStatistics& rightStats, const double outputRows, const double outputByteSize, EJoinAlgoType joinAlgo) const  {
    Y_UNUSED(outputByteSize);

    switch(joinAlgo) {
        case EJoinAlgoType::MapJoin:
            return 1.5 * (leftStats.Nrows + 1.8 * rightStats.Nrows + outputRows);
        case EJoinAlgoType::GraceJoin:
            return 1.5 * (leftStats.Nrows + 2.0 * rightStats.Nrows + outputRows);
        default:
            Y_ENSURE(false, "Illegal join type encountered");
            return 0;
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
        AddHandler(0, &TCoEquiJoin::Match, HNDL(RewriteStreamEquiJoinWithLookup));
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
                NDq::TSpillingSettings spillingSettings(Config->GetEnabledSpillingNodes());
                return DqRewriteAggregate(node, ctx, TypesCtx, true, Config->UseAggPhases.Get().GetOrElse(false), Config->UseFinalizeByKey.Get().GetOrElse(false), spillingSettings.IsAggregationSpillingEnabled());
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

    TDqLookupSourceWrap LookupSourceFromSource(TDqSourceWrap source, TExprContext& ctx) {
        return Build<TDqLookupSourceWrap>(ctx, source.Pos())
                .Input(source.Input())
                .DataSource(source.DataSource())
                .RowType(source.RowType())
                .Settings(source.Settings())
            .Done();
    }

    TDqLookupSourceWrap LookupSourceFromRead(TDqReadWrap read, TExprContext& ctx){ //temp replace with yt source
        IDqOptimization* dqOptimization = GetDqOptCallback(read.Input());
        YQL_ENSURE(dqOptimization);
        auto lookupSourceWrap = dqOptimization->RewriteLookupRead(read.Input().Ptr(), ctx);
        YQL_ENSURE(lookupSourceWrap, "Lookup read is not supported");
        return TDqLookupSourceWrap(lookupSourceWrap);
    }

    TMaybeNode<TExprBase> RewriteStreamEquiJoinWithLookup(TExprBase node, TExprContext& ctx) {
        Y_UNUSED(ctx);
        const auto equiJoin = node.Cast<TCoEquiJoin>();
        if (equiJoin.ArgCount() != 4) { // 2 parties join
            return node;
        }
        const auto left = equiJoin.Arg(0).Cast<TCoEquiJoinInput>().List();
        const auto right = equiJoin.Arg(1).Cast<TCoEquiJoinInput>().List();
        const auto joinTuple = equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>();
        if (!IsStreamLookup(joinTuple)) {
            return node;
        }
        if (!right.Maybe<TDqSourceWrap>() && !right.Maybe<TDqReadWrap>()) {
            return node;
        }

        TDqLookupSourceWrap lookupSourceWrap =  right.Maybe<TDqSourceWrap>()
            ? LookupSourceFromSource(right.Cast<TDqSourceWrap>(), ctx)
            : LookupSourceFromRead(right.Cast<TDqReadWrap>(), ctx)
        ;

        return Build<TCoEquiJoin>(ctx, node.Pos())
                .Add(equiJoin.Arg(0))
                .Add<TCoEquiJoinInput>()
                        .List(lookupSourceWrap)
                        .Scope(equiJoin.Arg(1).Cast<TCoEquiJoinInput>().Scope())
                .Build()
                .Add(equiJoin.Arg(2))
                .Add(equiJoin.Arg(3))
            .Done();
    }

    TMaybeNode<TExprBase> OptimizeEquiJoinWithCosts(TExprBase node, TExprContext& ctx) {
        if (TypesCtx.CostBasedOptimizer != ECostBasedOptimizerType::Disable) {
            std::function<void(const TString&)> log = [&](auto str) {
                YQL_CLOG(INFO, ProviderDq) << str;
            };

            std::unique_ptr<IOptimizerNew> opt;
            TDqCBOProviderContext pctx(TypesCtx, Config);

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

            return DqOptimizeEquiJoinWithCosts(node, ctx, TypesCtx, 2, *opt, providerCollect);
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

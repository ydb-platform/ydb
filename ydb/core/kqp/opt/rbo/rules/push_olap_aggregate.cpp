#include <ydb/core/kqp/opt/rbo/kqp_olap_aggregate.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp {

bool TPushOlapAggregateRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    if (input->Kind != EOperator::Aggregate) {
        return false;
    }
    // Column shards compute partial aggregates, the final aggregate merges them.
    const auto& aggregate = CastOperator<TOpAggregate>(*input);
    return aggregate.GetAggregationPhase() == EOpPhase::Intermediate && !aggregate.IsPushedToOlap();
}

TIntrusivePtr<IOperator> TPushOlapAggregateRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (!ctx.KqpCtx.Config->HasOptEnableOlapPushdown() || !ctx.KqpCtx.Config->GetEnableOlapPushdownAggregate()) {
        return input;
    }

    const auto aggregate = CastOperator<TOpAggregate>(input);
    const auto olapAggregate = MatchOlapAggregate(*aggregate, ctx.ExprCtx);
    if (!olapAggregate) {
        return input;
    }
    // The read program replaces the read and the map, so nothing else may consume them.
    if (olapAggregate->Read->Parents.size() != 1 || (olapAggregate->Map && olapAggregate->Map->Parents.size() != 1)) {
        return input;
    }
    if (!ctx.KqpCtx.Config->GetEnableOlapScalarApply() && HasOlapAverageCast(*olapAggregate, ctx.ExprCtx)) {
        return input;
    }

    auto pushed = MakeIntrusive<TOpAggregate>(aggregate->GetInput(), aggregate->GetAggregationTraits(), aggregate->GetKeyColumns(),
                                              EOpPhase::Intermediate, aggregate->IsDistinctAll(), aggregate->Props, aggregate->Pos);
    pushed->SetPushedToOlap();
    YQL_CLOG(TRACE, ProviderKqp) << "Pushed OLAP aggregate: " << pushed->ToString(ctx.ExprCtx, props.InfoUnitRegistry);
    return pushed;
}

} // namespace NKikimr::NKqp

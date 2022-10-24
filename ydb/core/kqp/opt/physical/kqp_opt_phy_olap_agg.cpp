#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>

#include <ydb/library/yql/core/yql_opt_utils.h>

#include <vector>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

std::string GetColumnNameUnderAggregation(const TCoAggApply& aggApply, TExprContext& ctx) {
    auto extractorBody = aggApply.Extractor().Body();
    if (extractorBody.Maybe<TCoVoid>() && aggApply.Name() == "count_all") {
        return "*";
    }
    if (!extractorBody.Maybe<TCoMember>()) {
        YQL_CLOG(WARN, ProviderKqp) << "Expected TCoMember callable to get column under aggregation. Got: " << KqpExprToPrettyString(extractorBody, ctx);
        return "";
    }
    return extractorBody.Cast<TCoMember>().Name().StringValue();
}

std::string GetAggregationName(const TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TCoAtom>()) {
        YQL_CLOG(WARN, ProviderKqp) << "Expected TCoAtom as aggregation name. Got: " << KqpExprToPrettyString(node, ctx);
        return "";
    }
    return node.Cast<TCoAtom>().StringValue();
}

bool CanBePushedDown(const TExprBase& trait, TExprContext& ctx)
{
    if (!trait.Maybe<TCoAggApply>()) {
        YQL_CLOG(DEBUG, ProviderKqp) << "Only TCoAggApply can be pushed to column shard. Got: " << KqpExprToPrettyString(trait, ctx);
        return false;
    }
    auto aggApply = trait.Cast<TCoAggApply>();
    auto aggName = aggApply.Name();
    if (aggName == "count" || aggName == "count_all" || aggName == "sum") {
        return true;
    }
    YQL_CLOG(DEBUG, ProviderKqp) << "Unsupported type of aggregation: " << aggName.StringValue();
    return false;
}

} // anonymous namespace end

TExprBase KqpPushOlapAggregate(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx)
{
    if (!kqpCtx.Config->PushOlapProcess()) {
        return node;
    }

    if (!node.Maybe<TCoAggregateCombine>()) {
        return node;
    }

    auto aggCombine = node.Cast<TCoAggregateCombine>();
    auto maybeRead = aggCombine.Input().Maybe<TKqpReadOlapTableRanges>();
    if (!maybeRead) {
        maybeRead = aggCombine.Input().Maybe<TCoExtractMembers>().Input().Maybe<TKqpReadOlapTableRanges>();
    }

    if (!maybeRead) {
        return node;
    }

    auto read = maybeRead.Cast();

    auto aggs = Build<TKqpOlapAggOperationList>(ctx, node.Pos());
    // TODO: TMaybeNode<TKqpOlapAggOperation>;
    for (auto handler: aggCombine.Handlers()) {
        auto trait = handler.Trait();
        if (!CanBePushedDown(trait, ctx)) {
            return node;
        }
        auto aggApply = trait.Cast<TCoAggApply>();
        auto aggName = GetAggregationName(handler.ColumnName(), ctx);
        auto colName = GetColumnNameUnderAggregation(aggApply, ctx);
        if (aggName.empty() || colName.empty()) {
            return node;
        }
        auto aggOp = aggApply.Name();
        if (aggOp == "count_all") {
            aggOp = TCoAtom(ctx.NewAtom(node.Pos(), "count"));
        }

        aggs.Add<TKqpOlapAggOperation>()
            .Name().Build(aggName)
            .Type().Build(aggOp)
            .Column().Build(colName)
            .Build()
            .Done();
    }

    auto olapAgg = Build<TKqpOlapAgg>(ctx, node.Pos())
        .Input(read.Process().Args().Arg(0))
        .Aggregates(std::move(aggs.Done()))
        .KeyColumns(aggCombine.Keys())
        .Done();

    auto olapAggLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"row"})
        .Body<TExprApplier>()
            .Apply(olapAgg)
            .With(read.Process().Args().Arg(0), "row")
            .Build()
        .Done();

    auto newProcessLambda = ctx.FuseLambdas(olapAggLambda.Ref(), read.Process().Ref());

    YQL_CLOG(INFO, ProviderKqp) << "Pushed OLAP lambda: " << KqpExprToPrettyString(*newProcessLambda, ctx);

    auto newRead = Build<TKqpReadOlapTableRanges>(ctx, node.Pos())
        .Table(read.Table())
        .Ranges(read.Ranges())
        .Columns(read.Columns())
        .Settings(read.Settings())
        .ExplainPrompt(read.ExplainPrompt())
        .Process(newProcessLambda)
        .Done();

    return newRead;
}

TExprBase KqpPushOlapLength(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx)
{
    if (!kqpCtx.Config->PushOlapProcess()) {
        return node;
    }

    if (!node.Maybe<TDqPhyLength>().Input().Maybe<TKqpReadOlapTableRanges>()) {
        return node;
    }

    auto dqPhyLength = node.Cast<TDqPhyLength>();
    auto read = dqPhyLength.Input().Cast<TKqpReadOlapTableRanges>();

    if (read.Process().Body().Raw() != read.Process().Args().Arg(0).Raw()) {
        return node;
    }

    auto aggs = Build<TKqpOlapAggOperationList>(ctx, node.Pos());
    aggs.Add<TKqpOlapAggOperation>()
            .Name(dqPhyLength.Name())
            .Type().Build("count")
            .Column().Build("*")
            .Build()
            .Done();

    auto olapAgg = Build<TKqpOlapAgg>(ctx, node.Pos())
        .Input(read.Process().Body())
        .Aggregates(std::move(aggs.Done()))
        .KeyColumns(std::move(
            Build<TCoAtomList>(ctx, node.Pos())
            .Done()
            )
        )
        .Done();

    auto newProcessLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"row"})
        .Body<TExprApplier>()
            .Apply(olapAgg)
            .With(read.Process().Args().Arg(0), "row")
            .Build()
        .Done();

    YQL_CLOG(INFO, ProviderKqp) << "Pushed OLAP lambda: " << KqpExprToPrettyString(newProcessLambda, ctx);

    auto newRead = Build<TKqpReadOlapTableRanges>(ctx, node.Pos())
        .Table(read.Table())
        .Ranges(read.Ranges())
        .Columns(read.Columns())
        .Settings(read.Settings())
        .ExplainPrompt(read.ExplainPrompt())
        .Process(newProcessLambda)
        .Done();

    auto member = Build<TCoMap>(ctx, node.Pos())
        .Input(newRead)
        .Lambda()
            .Args({"row"})
            .Body<TCoMember>()
                .Struct("row")
                .Name(dqPhyLength.Name())
                .Build()
            .Build()
        .Done();

    return member;
}

} // namespace NKikimr::NKqp::NOpt
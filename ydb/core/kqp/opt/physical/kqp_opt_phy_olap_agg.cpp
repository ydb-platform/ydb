#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>

#include <ydb/core/formats/arrow/ssa_runtime_version.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/actors/core/log.h>

#include <vector>
#include <unordered_set>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

static const std::unordered_set<std::string> SupportedAggFuncs = {
    "count",
    "count_all",
    "sum",
    "min",
    "max",
    "avg",
    "some"
};

struct TAggInfo {
    TAggInfo(const std::string& aggName, const std::string& colName, const std::string& opType, bool isOptional)
        : AggName(aggName)
        , ColName(colName)
        , OpType(opType)
        , IsOptional(isOptional)
    {}

    std::string AggName;
    std::string ColName;
    std::string OpType;
    bool IsOptional;
};

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
    if (SupportedAggFuncs.find(aggName.StringValue()) != SupportedAggFuncs.end()) {
        return true;
    }
    YQL_CLOG(DEBUG, ProviderKqp) << "Unsupported type of aggregation: " << aggName.StringValue();
    return false;
}

std::vector<TAggInfo> CollectAggInfos(const TCoAggregateTupleList& handlers, TExprContext& ctx) {
    std::vector<TAggInfo> res;
    for (auto handler : handlers) {
        auto trait = handler.Trait();
        if (!CanBePushedDown(trait, ctx)) {
            res.clear();
            return res;
        }
        auto aggApply = trait.Cast<TCoAggApply>();
        auto aggName = GetAggregationName(handler.ColumnName(), ctx);
        auto colName = GetColumnNameUnderAggregation(aggApply, ctx);
        bool isOptional = aggApply.Ptr()->GetTypeAnn()->IsOptionalOrNull();
        if (aggName.empty() || colName.empty()) {
            res.clear();
            return res;
        }
        auto aggOp = aggApply.Name().StringValue();
        res.emplace_back(aggName, colName, aggOp, isOptional);
    }
    return res;
}

TExprBase GenerateResultTupleForAvg(const TAggInfo& aggInfo, const TExprBase& itemArg, const TPositionHandle& nodePos, TExprContext& ctx) {
    // If SUM is not null, generate Just(convert(sum as double), count)
    // If SUM is null, return null
    auto sumMember = Build<TCoMember>(ctx, nodePos)
        .Struct(itemArg)
        .Name<TCoAtom>().Build(aggInfo.AggName + "_sum")
        .Done();
    auto cntMember = Build<TCoMember>(ctx, nodePos)
        .Struct(itemArg)
        .Name<TCoAtom>().Build(aggInfo.AggName + "_cnt")
        .Done();

    TMaybeNode<TExprBase> value;
    if (aggInfo.IsOptional) {
        value = Build<TCoIfPresent>(ctx, nodePos)
            .Optional(sumMember)
            .PresentHandler<TCoLambda>()
                .Args({"sumAgg"})
                .Body<TCoJust>()
                    .Input<TExprList>()
                        .Add<TCoConvert>()
                            .Input("sumAgg")
                            // For Decimal and Interval (currently unsupported in CS)
                            // need to change target type accoringly to aggregate.yql
                            .Type().Build("Double")
                            .Build()
                        .Add(cntMember)
                        .Build()
                    .Build()
                .Build()
            .MissingValue<TCoNull>()
                .Build()
            .Done();
    } else {
        value = Build<TExprList>(ctx, nodePos)
            .Add<TCoConvert>()
                .Input(sumMember)
                // For Decimal and Interval (currently unsupported in CS)
                // need to change target type accoringly to aggregate.yql
                .Type().Build("Double")
                .Build()
            .Add(cntMember)
        .Done();
    }
    return Build<TCoNameValueTuple>(ctx, nodePos)
        .Name<TCoAtom>().Build(aggInfo.AggName)
        .Value(value.Cast())
        .Done();
}

TExprBase BuildAvgResultProcessing(const std::vector<TAggInfo>& aggInfos, const TCoAtomList& groupByKeys,
    const TExprBase& input, const TPositionHandle& nodePos, TExprContext& ctx)
{
    const auto itemArg = Build<TCoArgument>(ctx, nodePos)
        .Name("item")
        .Done();
    TVector<TExprBase> structMembers;
    for (auto aggInfo : aggInfos) {
        if (aggInfo.OpType == "avg") {
            structMembers.emplace_back(
                GenerateResultTupleForAvg(aggInfo, itemArg, nodePos, ctx)
            );
        } else {
            structMembers.emplace_back(
                Build<TCoNameValueTuple>(ctx, nodePos)
                    .Name<TCoAtom>().Build(aggInfo.AggName)
                    .Value<TCoMember>()
                        .Struct(itemArg)
                        .Name<TCoAtom>().Build(aggInfo.AggName)
                        .Build()
                    .Done()
            );
        }
    }

    // Add GROUP BY keys
    for (auto key : groupByKeys) {
        structMembers.emplace_back(
            Build<TCoNameValueTuple>(ctx, nodePos)
                .Name<TCoAtom>().Build(key)
                .Value<TCoMember>()
                    .Struct(itemArg)
                    .Name<TCoAtom>().Build(key)
                    .Build()
                .Done()
        );
    }

    return Build<TCoMap>(ctx, nodePos)
        .Input(input)
        .Lambda()
            .Args({itemArg})
            .Body<TCoAsStruct>()
                .Add(structMembers)
                .Build()
            .Build()
        .Done();
}

} // anonymous namespace end

template <class TReadClass>
TExprBase KqpPushDownOlapGroupByKeysImpl(TExprBase node, TExprContext& ctx, bool& applied) {
    applied = false;
    auto aggCombine = node.Cast<TCoAggregateCombine>();
    if (aggCombine.Keys().Empty()) {
        return node;
    }

    auto maybeRead = aggCombine.Input().Maybe<TReadClass>();
    if (!maybeRead) {
        maybeRead = aggCombine.Input().Maybe<TCoFlatMap>().Input().Maybe<TReadClass>();
    }

    if (!maybeRead) {
        return node;
    }

    if (NYql::HasSetting(maybeRead.Settings().Ref(), TKqpReadTableSettings::GroupByFieldNames)) {
        return node;
    }
    auto newSettings = NYql::AddSetting(maybeRead.Settings().Cast().Ref(), maybeRead.Settings().Cast().Pos(),
        TString(TKqpReadTableSettings::GroupByFieldNames.data(), TKqpReadTableSettings::GroupByFieldNames.size()), aggCombine.Keys().Ptr(), ctx);
    if (auto read = aggCombine.Input().Maybe<TReadClass>()) {
        applied = true;
        return
            Build<TCoAggregateCombine>(ctx, node.Pos()).InitFrom(node.Cast<TCoAggregateCombine>())
               .Input<TReadClass>().InitFrom(read.Cast())
                   .Settings(newSettings)
               .Build()
           .Done();
    } else if (auto read = aggCombine.Input().Maybe<TCoFlatMap>().Input().Maybe<TReadClass>()) {
        applied = true;
        return
            Build<TCoAggregateCombine>(ctx, node.Pos()).InitFrom(node.Cast<TCoAggregateCombine>())
                .Input<TCoFlatMap>().InitFrom(aggCombine.Input().Maybe<TCoFlatMap>().Cast())
                    .Input<TReadClass>().InitFrom(read.Cast())
            .Settings(newSettings)
                    .Build()
                .Build()
            .Done();
    } else {
        return node;
    }
}

TExprBase KqpPushDownOlapGroupByKeys(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (NKikimr::NSsa::RuntimeVersion < 2U) {
        // We introduced aggregate pushdown in v2 of SSA program
        return node;
    }

    if (!kqpCtx.Config->HasOptEnableOlapPushdown() || !kqpCtx.Config->HasOptEnableOlapProvideComputeSharding()) {
        return node;
    }

    if (!node.Maybe<TCoAggregateCombine>()) {
        return node;
    }
    bool applied = false;
    auto result = KqpPushDownOlapGroupByKeysImpl<TKqpReadOlapTableRanges>(node, ctx, applied);
    if (applied) {
        return result;
    }
    result = KqpPushDownOlapGroupByKeysImpl<TKqlReadTableRanges>(node, ctx, applied);
    if (applied) {
        return result;
    }
    return node;
}

TExprBase KqpPushOlapAggregate(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx)
{
    if (NKikimr::NSsa::RuntimeVersion < 2U) {
        // We introduced aggregate pushdown in v2 of SSA program
        return node;
    }

    if (!kqpCtx.Config->HasOptEnableOlapPushdown()) {
        return node;
    }

    if (!node.Maybe<TCoAggregateCombine>()) {
        return node;
    }

    auto aggCombine = node.Cast<TCoAggregateCombine>();

    if (aggCombine.Handlers().Size() == 0) {
        return node;
    }

    auto maybeRead = aggCombine.Input().Maybe<TKqpReadOlapTableRanges>();
    if (!maybeRead) {
        maybeRead = aggCombine.Input().Maybe<TCoExtractMembers>().Input().Maybe<TKqpReadOlapTableRanges>();
    }

    if (!maybeRead) {
        return node;
    }

    auto read = maybeRead.Cast();
    auto aggs = Build<TKqpOlapAggOperationList>(ctx, node.Pos());

    auto aggInfos = CollectAggInfos(aggCombine.Handlers(), ctx);
    if (aggInfos.empty()) {
        return node;
    }

    bool hasAvgAgg = false;
    for (auto aggInfo : aggInfos) {
        if (aggInfo.OpType == "count_all") {
            aggInfo.OpType = TCoAtom(ctx.NewAtom(node.Pos(), "count"));
        }

        if (aggInfo.OpType == "avg") {
            aggs.Add<TKqpOlapAggOperation>()
                .Name().Build(aggInfo.AggName + "_sum")
                .Type().Build("sum")
                .Column().Build(aggInfo.ColName)
                .Build()
                .Done();
            aggs.Add<TKqpOlapAggOperation>()
                .Name().Build(aggInfo.AggName + "_cnt")
                .Type().Build("count")
                .Column().Build(aggInfo.ColName)
                .Build()
                .Done();
            hasAvgAgg = true;
        } else {
            aggs.Add<TKqpOlapAggOperation>()
                .Name().Build(aggInfo.AggName)
                .Type().Build(aggInfo.OpType)
                .Column().Build(aggInfo.ColName)
                .Build()
                .Done();
        }
    }

    auto olapAgg = Build<TKqpOlapAgg>(ctx, node.Pos())
        .Input(read.Process().Args().Arg(0))
        .Aggregates(std::move(aggs.Done()))
        .KeyColumns(aggCombine.Keys())
        .Done();

    auto olapAggLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"olap_agg_row"})
        .Body<TExprApplier>()
            .Apply(olapAgg)
            .With(read.Process().Args().Arg(0), "olap_agg_row")
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

    if (hasAvgAgg) {
        return BuildAvgResultProcessing(aggInfos, aggCombine.Keys(), newRead, node.Pos(), ctx);
    }

    return newRead;
}

TExprBase KqpPushOlapLength(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx)
{
    if (NKikimr::NSsa::RuntimeVersion < 2U) {
        // We introduced aggregate pushdown in v2 of SSA program
        return node;
    }

    if (!kqpCtx.Config->HasOptEnableOlapPushdown()) {
        return node;
    }

    if (!node.Maybe<TDqPhyLength>()) {
        return node;
    }

    auto dqPhyLength = node.Cast<TDqPhyLength>();
    auto maybeRead = dqPhyLength.Input().Maybe<TKqpReadOlapTableRanges>();
    if (!maybeRead) {
        return node;
    }

    auto read = maybeRead.Cast();
    auto aggs = Build<TKqpOlapAggOperationList>(ctx, node.Pos());
    aggs.Add<TKqpOlapAggOperation>()
            .Name(dqPhyLength.Name())
            .Type().Build("count")
            .Column().Build("*")
            .Build()
            .Done();

    auto olapAgg = Build<TKqpOlapAgg>(ctx, node.Pos())
        .Input(read.Process().Args().Arg(0))
        .Aggregates(std::move(aggs.Done()))
        .KeyColumns(std::move(
            Build<TCoAtomList>(ctx, node.Pos())
            .Done()
            )
        )
        .Done();

    auto olapAggLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"olap_agg_row"})
        .Body<TExprApplier>()
            .Apply(olapAgg)
            .With(read.Process().Args().Arg(0), "olap_agg_row")
            .Build()
        .Done();

    auto newProcessLambda = ctx.FuseLambdas(olapAggLambda.Ref(), read.Process().Ref());
    YQL_CLOG(INFO, ProviderKqp) << "Pushed OLAP lambda: " << KqpExprToPrettyString(*newProcessLambda, ctx);

    return Build<TKqpReadOlapTableRanges>(ctx, node.Pos())
        .Table(read.Table())
        .Ranges(read.Ranges())
        .Columns(read.Columns())
        .Settings(read.Settings())
        .ExplainPrompt(read.ExplainPrompt())
        .Process(newProcessLambda)
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

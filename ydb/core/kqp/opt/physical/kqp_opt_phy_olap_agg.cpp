#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>

#include <ydb/library/yql/core/yql_opt_utils.h>

#include <vector>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

enum class EAggType {
    Count,
    Some
};

struct TAggInfo {
    std::string AggName;
    std::string ColName;
    EAggType Type;
};

bool ContainsConstOnly(const TExprBase& node) {
    return node.Maybe<TCoDataCtor>().IsValid();
}

bool ContainsSimpleColumnOnly(const TExprBase& node, const TExprBase& parent) {
    if (!parent.Maybe<TCoInputBase>()) {
        return false;
    }
    auto input = parent.Cast<TCoInputBase>().Input();
    if (auto maybeExprList = node.Maybe<TExprList>()) {
        for (auto expr : maybeExprList.Cast()) {
            if (!expr.Maybe<TCoMember>() || expr.Cast<TCoMember>().Struct().Raw() != input.Raw()) {
                return false;
            }
        }
        return true;
    }
    return node.Maybe<TCoMember>().IsValid() && node.Cast<TCoMember>().Struct().Raw() == input.Raw();
}

std::vector<std::string> GetGroupByCols(const TExprBase& keySelectorBody, const TExprBase& parent) {
    std::vector<std::string> res;
    if (!ContainsSimpleColumnOnly(keySelectorBody, parent)) {
        YQL_CLOG(DEBUG, ProviderKqp) << "For aggregate push down optimization in GROUP BY column list should be Member callables only.";
        return res;
    }
    if (auto maybeMember = keySelectorBody.Maybe<TCoMember>()) {
        res.push_back(keySelectorBody.Cast<TCoMember>().Name().StringValue());
    } else if (auto maybeExprList = keySelectorBody.Maybe<TExprList>()) {
        for (auto expr : maybeExprList.Cast()) {
            res.push_back(expr.Cast<TCoMember>().Name().StringValue());
        }
    }
    return res;
}

std::vector<TAggInfo> GetAggregationsFromInit(const TExprBase& node) {
    std::vector<TAggInfo> res;
    if (!node.Maybe<TCoAsStruct>()) {
        return res;
    }
    for (auto item : node.Cast<TCoAsStruct>()) {
        auto tuple = item.Cast<TCoNameValueTuple>();
        auto tupleValue = tuple.Value();
        if (tupleValue.Maybe<TCoAggrCountInit>()) {
            auto aggrCntInit = tupleValue.Cast<TCoAggrCountInit>();
            if (aggrCntInit.Value().Maybe<TCoMember>()) {
                TAggInfo aggInfo;
                aggInfo.AggName = tuple.Name();
                aggInfo.Type = EAggType::Count;
                aggInfo.ColName = aggrCntInit.Value().Cast<TCoMember>().Name();
                res.push_back(aggInfo);
            }
        } else {
            YQL_CLOG(DEBUG, ProviderKqp) << "Unsupported aggregation type in init handler.";
            res.clear();
            return res;
        }
    }
    return res;
}

std::vector<TAggInfo> GetAggregationsFromUpdate(const TExprBase& node) {
    std::vector<TAggInfo> res;
    if (!node.Maybe<TCoAsStruct>()) {
        return res;
    }
    for (auto item : node.Cast<TCoAsStruct>()) {
        auto tuple = item.Cast<TCoNameValueTuple>();
        auto tupleValue = tuple.Value();
        if (auto maybeAggrCntUpd = tupleValue.Maybe<TCoAggrCountUpdate>()) {
            if (maybeAggrCntUpd.Cast().Value().Maybe<TCoMember>()) {
                TAggInfo aggInfo;
                aggInfo.Type = EAggType::Count;
                aggInfo.AggName = tuple.Name();
                res.push_back(aggInfo);
            }
        } else {
            YQL_CLOG(DEBUG, ProviderKqp) << "Unsupported aggregation type in update handler.";
            res.clear();
            return res;
        }
    }
    return res;
}

} // anonymous namespace end

TExprBase KqpPushOlapAggregate(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx)
{
    if (!kqpCtx.Config->PushOlapProcess()) {
        return node;
    }

    if (!node.Maybe<TCoCombineByKey>().Input().Maybe<TKqpReadOlapTableRanges>()) {
        return node;
    }

    auto combineKey = node.Cast<TCoCombineByKey>();
    auto read = combineKey.Input().Cast<TKqpReadOlapTableRanges>();

    if (read.Process().Body().Raw() != read.Process().Args().Arg(0).Raw()) {
        return node;
    }

    auto keySelectorBody = combineKey.KeySelectorLambda().Cast<TCoLambda>().Body();
    if (!ContainsSimpleColumnOnly(keySelectorBody, combineKey) && !ContainsConstOnly(keySelectorBody)) {
        return node;
    }
    auto aggKeyCols = Build<TCoAtomList>(ctx, node.Pos());
    auto groupByCols = GetGroupByCols(keySelectorBody, combineKey);
    for (auto groupByCol : groupByCols) {
        aggKeyCols.Add<TCoAtom>()
            .Build(groupByCol)
        .Done();
    }

    auto initHandlerBody = combineKey.InitHandlerLambda().Cast<TCoLambda>().Body();
    auto aggInits = GetAggregationsFromInit(initHandlerBody);

    auto updateHandlerBody = combineKey.UpdateHandlerLambda().Cast<TCoLambda>().Body();
    auto aggUpdates = GetAggregationsFromUpdate(updateHandlerBody);

    auto finishHandlerBody = combineKey.FinishHandlerLambda().Cast<TCoLambda>().Body();
    if (aggInits.empty() || aggInits.size() != aggUpdates.size()) {
        return node;
    }

    for (size_t i = 0; i != aggInits.size(); ++i) {
        if (aggInits[i].Type != aggUpdates[i].Type) {
           YQL_CLOG(DEBUG, ProviderKqp) << "Different aggregation type in init and update handlers in aggregate push-down optimization!";
           return node;
        }
    }

    auto aggs = Build<TKqpOlapAggOperationList>(ctx, node.Pos());
    // TODO: TMaybeNode<TKqpOlapAggOperation>;
    for (size_t i = 0; i != aggInits.size(); ++i) {
        std::string aggType;
        switch (aggInits[i].Type) {
            case EAggType::Count:
            {
                aggType = "count";
                break;
            }
            case EAggType::Some:
            {
                aggType = "some";
                break;
            }
            default:
            {
                YQL_ENSURE(false, "Unsupported type of aggregation!"); //  add aggInits[i].Type
                return node;
            }
        }
        aggs.Add<TKqpOlapAggOperation>()
            .Name().Build(aggInits[i].AggName)
            .Type().Build(aggType)
            .Column().Build(aggInits[i].ColName)
            .Build()
            .Done();
    }

    auto olapAgg = Build<TKqpOlapAgg>(ctx, node.Pos())
        .Input(read.Process().Body())
        .Aggregates(std::move(aggs.Done()))
        .KeyColumns(std::move(aggKeyCols.Done()))
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
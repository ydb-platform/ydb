#include "kqp_columns_getter_transformer.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/library/yql/core/yql_statistics.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>

namespace NKikimr::NKqp {

using namespace NThreading;
using namespace NYql;

void TKqpColumnsGetterTransformer::PropagateTableToLambdaArgument(const TExprNode::TPtr& input) {
    if (input->ChildrenSize() < 2) {
        return;
    }

    auto callableInput = input->ChildRef(0);

   
    for (size_t i = 1; i < input->ChildrenSize(); ++i) {
        auto maybeLambda = TExprBase(input->ChildRef(i));
        if (!maybeLambda.Maybe<TCoLambda>()) {
            continue;
        }

        auto lambda = maybeLambda.Cast<TCoLambda>();
        if (!lambda.Args().Size()){
            continue;
        }

        if (callableInput->IsList()){
            for (size_t j = 0; j < callableInput->ChildrenSize(); ++j){
                TableByExprNode[lambda.Args().Arg(j).Ptr()] = TableByExprNode[callableInput->Child(j)];  
            }
        } else {
            TableByExprNode[lambda.Args().Arg(0).Ptr()] = TableByExprNode[callableInput.Get()];
        }
    }
}

IGraphTransformer::TStatus TKqpColumnsGetterTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    Y_UNUSED(ctx);
    
    output = input;
    auto optLvl = Config->CostBasedOptimizationLevel.Get().GetOrElse(TDqSettings::TDefault::CostBasedOptimizationLevel);
    auto enableColumnStats = Config->FeatureFlags.GetEnableColumnStatistics();
    if (!(optLvl > 0 && enableColumnStats)) {
        return IGraphTransformer::TStatus::Ok;
    }
    
    VisitExprLambdasLast(
        input, 
        [&](const TExprNode::TPtr& input) {
            BeforeLambdas(input) || BeforeLambdasUnmatched(input);

            if (input->IsCallable()) {
                PropagateTableToLambdaArgument(input);
            }

            return true;
        },
        [&](const TExprNode::TPtr& input) {
            return AfterLambdas(input) || AfterLambdasUnmatched(input);
        }
    );

    struct TTableMeta {
        TString TableName;
        THashMap<ui32, TString> ColumnNameByTag;
    };
    THashMap<TPathId, TTableMeta> tableMetaByPathId;

    // TODO: Add other statistics, not only COUNT_MIN_SKETCH.
    auto getStatisticsRequest = MakeHolder<NStat::TEvStatistics::TEvGetStatistics>();
    getStatisticsRequest->StatType = NKikimr::NStat::EStatType::COUNT_MIN_SKETCH;  

    for (const auto& [table, columns]: ColumnsByTableName) {
        auto tableMeta = Tables.GetTable(Cluster, table).Metadata;
        auto& columnsMeta = tableMeta->Columns;

        auto pathId = TPathId(tableMeta->PathId.OwnerId(), tableMeta->PathId.TableId());
        for (const auto& column: columns) {
            Y_ENSURE(columns.contains(column), "There is no " + column + " in column meta!");

            NKikimr::NStat::TRequest req;
            req.ColumnTag = columnsMeta[column].Id;
            req.PathId = pathId;
            getStatisticsRequest->StatRequests.push_back(req);

            tableMetaByPathId[pathId].TableName = table;
            tableMetaByPathId[pathId].ColumnNameByTag[req.ColumnTag.value()] = column;
        }
    }

    using TRequest = NStat::TEvStatistics::TEvGetStatistics;
    using TResponse = NStat::TEvStatistics::TEvGetStatisticsResult;
    struct TResult : public NYql::IKikimrGateway::TGenericResult {
        THashMap<TString, TOptimizerStatistics::TColumnStatMap> columnStatisticsByTableName;
    };

    auto promise = NewPromise<TResult>();
    auto callback = [tableMetaByPathId = std::move(tableMetaByPathId)]
    (TPromise<TResult> promise, NStat::TEvStatistics::TEvGetStatisticsResult&& response) mutable {
        bool isOk = response.Success;
        Y_ENSURE(isOk);
        
        THashMap<TString, TOptimizerStatistics::TColumnStatMap> columnStatisticsByTableName;

        for (auto&& stat: response.StatResponses) {
            auto meta = tableMetaByPathId[stat.Req.PathId];
            auto columnName = meta.ColumnNameByTag[stat.Req.ColumnTag.value()];
            auto& columnStatistics = columnStatisticsByTableName[meta.TableName].Data[columnName];
            columnStatistics.CountMinSketch = std::move(stat.CountMinSketch.CountMin);
        }

        promise.SetValue(TResult{.columnStatisticsByTableName = std::move(columnStatisticsByTableName)});
    };
    auto statServiceId = NStat::MakeStatServiceID(ActorSystem->NodeId);
    IActor* requestHandler = 
        new TActorRequestHandler<TRequest, TResponse, TResult>(statServiceId, getStatisticsRequest.Release(), promise, callback);
    auto actorId = ActorSystem
        ->Register(requestHandler, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
    Y_UNUSED(actorId);

    auto columnStatisticsByTableName = promise.GetFuture().GetValueSync();

    return IGraphTransformer::TStatus::Ok;
}

bool TKqpColumnsGetterTransformer::BeforeLambdas(const TExprNode::TPtr& input) {
    bool matched = true;
    
    if (TKqpTable::Match(input.Get())) {
        TableByExprNode[input.Get()] = input.Get();
    } else if (auto maybeStreamLookup = TExprBase(input).Maybe<TKqpCnStreamLookup>()) {
        TableByExprNode[input.Get()] = maybeStreamLookup.Cast().Table().Ptr();
    } else {
        matched = false;
    }

    return matched;
}

bool TKqpColumnsGetterTransformer::BeforeLambdasUnmatched(const TExprNode::TPtr& input) {
    for (const auto& node: input->Children()) {
        if (TableByExprNode.contains(node)) {
            TableByExprNode[input.Get()] = TableByExprNode[node];
            return true;
        }
    }

    return true;
}

bool TKqpColumnsGetterTransformer::AfterLambdas(const TExprNode::TPtr& input) {
    bool matched = true;

    if (
        TCoFilterBase::Match(input.Get()) ||
        TCoFlatMapBase::Match(input.Get()) && IsPredicateFlatMap(TExprBase(input).Cast<TCoFlatMapBase>().Lambda().Body().Ref())
    ) {
        auto computer = NDq::TPredicateSelectivityComputer(nullptr, true);
    
        if (TCoFilterBase::Match(input.Get())) {
            computer.Compute(TExprBase(input).Cast<TCoFilterBase>().Lambda().Body());
        } else if (TCoFlatMapBase::Match(input.Get())) {
            computer.Compute(TExprBase(input).Cast<TCoFlatMapBase>().Lambda().Body());
        } else {
            Y_ENSURE(false);
        }

        auto columnStatsUsedMembers = computer.GetColumnStatsUsedMembers();
        for (const auto& item: columnStatsUsedMembers.Data) {
            auto exprNode = TExprBase(item.Member).Ptr();
            if (!TableByExprNode.contains(exprNode) || TableByExprNode[exprNode] == nullptr) {
                continue;
            }

            auto table = TExprBase(TableByExprNode[exprNode]).Cast<TKqpTable>().Path().StringValue();
            auto column = item.Member.Name().StringValue();
            size_t pointPos = column.find('.'); // table.column
            if (pointPos != TString::npos) {
                column = column.substr(pointPos + 1);
            }

            Cout << table << " " << column << input.Get()->Dump() << Endl;
            ColumnsByTableName[table].insert(std::move(column));
        }
    } else {
        matched = false;
    }

    return matched;
}

bool TKqpColumnsGetterTransformer::AfterLambdasUnmatched(const TExprNode::TPtr& input) {
    if (TableByExprNode.contains(input.Get())) {
        return true;
    }

    for (const auto& node: input->Children()) {
        if (TableByExprNode.contains(node)) {
            TableByExprNode[input.Get()] = TableByExprNode[node];
            return true;
        }
    }

    return true;
}

TAutoPtr<IGraphTransformer> CreateKqpColumnsGetterTransformer(
    const TKikimrConfiguration::TPtr& config,
    TKikimrTablesData& tables,
    TString cluster,
    TActorSystem* actorSystem
) {
    return THolder<IGraphTransformer>(new TKqpColumnsGetterTransformer(config, tables, cluster, actorSystem));
}

} // end of NKikimr::NKqp

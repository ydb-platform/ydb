#include "kqp_column_statistics_requester.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/library/yql/core/yql_statistics.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NKikimr::NKqp {

using namespace NThreading;
using namespace NYql;

void TKqpColumnStatisticsRequester::PropagateTableToLambdaArgument(const TExprNode::TPtr& input) {
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
                KqpTableByExprNode[lambda.Args().Arg(j).Ptr()] = KqpTableByExprNode[callableInput->Child(j)];  
            }
        } else {
            KqpTableByExprNode[lambda.Args().Arg(0).Ptr()] = KqpTableByExprNode[callableInput.Get()];
        }
    }
}

IGraphTransformer::TStatus TKqpColumnStatisticsRequester::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
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

    if (ColumnsByTableName.empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

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
            if (TypesCtx.ColumnStatisticsByTableName.contains(table) && TypesCtx.ColumnStatisticsByTableName[table]->Data.contains(column)) {
                continue;
            }

            if (!columns.contains(column)) {
                YQL_CLOG(DEBUG, ProviderKikimr) << "Table: " + table + " doesn't contain " + column + " to request for column statistics";
            }

            NKikimr::NStat::TRequest req;
            req.ColumnTag = columnsMeta[column].Id;
            req.PathId = pathId;
            getStatisticsRequest->StatRequests.push_back(std::move(req));

            tableMetaByPathId[pathId].TableName = table;
            tableMetaByPathId[pathId].ColumnNameByTag[req.ColumnTag.value()] = column;
        }
    }

    if (getStatisticsRequest->StatRequests.empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

    using TRequest = NStat::TEvStatistics::TEvGetStatistics;
    using TResponse = NStat::TEvStatistics::TEvGetStatisticsResult;

    AsyncReadiness = NewPromise<void>();
    auto promise = NewPromise<TColumnStatisticsResponse>();
    auto callback = [tableMetaByPathId = std::move(tableMetaByPathId)]
    (TPromise<TColumnStatisticsResponse> promise, NStat::TEvStatistics::TEvGetStatisticsResult&& response) mutable {
        if (!response.Success) {
            promise.SetValue(NYql::NCommon::ResultFromError<TColumnStatisticsResponse>("can't get column statistics!"));
            return;
        }
        
        THashMap<TString, TOptimizerStatistics::TColumnStatMap> columnStatisticsByTableName;

        for (auto&& stat: response.StatResponses) {
            auto meta = tableMetaByPathId[stat.Req.PathId];
            auto columnName = meta.ColumnNameByTag[stat.Req.ColumnTag.value()];
            auto& columnStatistics = columnStatisticsByTableName[meta.TableName].Data[columnName];
            columnStatistics.CountMinSketch = std::move(stat.CountMinSketch.CountMin);
        }

        promise.SetValue(TColumnStatisticsResponse{.ColumnStatisticsByTableName = std::move(columnStatisticsByTableName)});
    };
    auto statServiceId = NStat::MakeStatServiceID(ActorSystem->NodeId);
    IActor* requestHandler = 
        new TActorRequestHandler<TRequest, TResponse, TColumnStatisticsResponse>(statServiceId, getStatisticsRequest.Release(), promise, callback);
    ActorSystem
        ->Register(requestHandler, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);

    promise.GetFuture().Subscribe([this](auto result){ ColumnStatisticsResponse = result.ExtractValue(); AsyncReadiness.SetValue(); });

    return TStatus::Async;
}

IGraphTransformer::TStatus TKqpColumnStatisticsRequester::DoApplyAsyncChanges(TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
    Y_ENSURE(AsyncReadiness.IsReady() && ColumnStatisticsResponse.has_value());

    if (!ColumnStatisticsResponse->Issues().Empty()) {
        TStringStream ss; ColumnStatisticsResponse->Issues().PrintTo(ss);
        YQL_CLOG(TRACE, ProviderKikimr) << "Can't load columns statistics for request: " << ss.Str();
        return IGraphTransformer::TStatus::Ok;
    }

    for (auto&& [tableName, columnStatistics]:  ColumnStatisticsResponse->ColumnStatisticsByTableName) {
        TypesCtx.ColumnStatisticsByTableName.insert(
            {std::move(tableName), new TOptimizerStatistics::TColumnStatMap(std::move(columnStatistics))}
        );
    }

    return TStatus::Ok;
}

TFuture<void> TKqpColumnStatisticsRequester::DoGetAsyncFuture(const TExprNode&) {
    return AsyncReadiness.GetFuture();
}

bool TKqpColumnStatisticsRequester::BeforeLambdas(const TExprNode::TPtr& input) {
    bool matched = true;
    
    if (TKqpTable::Match(input.Get())) {
        KqpTableByExprNode[input.Get()] = input.Get();
    } else if (auto maybeStreamLookup = TExprBase(input).Maybe<TKqpCnStreamLookup>()) {
        KqpTableByExprNode[input.Get()] = maybeStreamLookup.Cast().Table().Ptr();
    } else {
        matched = false;
    }

    return matched;
}

bool TKqpColumnStatisticsRequester::BeforeLambdasUnmatched(const TExprNode::TPtr& input) {
    for (const auto& node: input->Children()) {
        if (KqpTableByExprNode.contains(node)) {
            KqpTableByExprNode[input.Get()] = KqpTableByExprNode[node];
            return true;
        }
    }

    return true;
}

bool TKqpColumnStatisticsRequester::AfterLambdas(const TExprNode::TPtr& input) {
    bool matched = true;

    if (
        TCoFilterBase::Match(input.Get()) ||
        TCoFlatMapBase::Match(input.Get()) && IsPredicateFlatMap(TExprBase(input).Cast<TCoFlatMapBase>().Lambda().Body().Ref())
    ) {
        std::shared_ptr<TOptimizerStatistics> dummyStats = nullptr;
        auto computer = NDq::TPredicateSelectivityComputer(dummyStats, true);
    
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
            if (!KqpTableByExprNode.contains(exprNode) || KqpTableByExprNode[exprNode] == nullptr) {
                continue;
            }

            auto table = TExprBase(KqpTableByExprNode[exprNode]).Cast<TKqpTable>().Path().StringValue();
            auto column = item.Member.Name().StringValue();
            size_t pointPos = column.find('.'); // table.column
            if (pointPos != TString::npos) {
                column = column.substr(pointPos + 1);
            }

            ColumnsByTableName[table].insert(std::move(column));
        }
    } else {
        matched = false;
    }

    return matched;
}

bool TKqpColumnStatisticsRequester::AfterLambdasUnmatched(const TExprNode::TPtr& input) {
    if (KqpTableByExprNode.contains(input.Get())) {
        return true;
    }

    for (const auto& node: input->Children()) {
        if (KqpTableByExprNode.contains(node)) {
            KqpTableByExprNode[input.Get()] = KqpTableByExprNode[node];
            return true;
        }
    }

    return true;
}

TAutoPtr<IGraphTransformer> CreateKqpColumnStatisticsRequester(
    const TKikimrConfiguration::TPtr& config,
    TTypeAnnotationContext& typesCtx,
    TKikimrTablesData& tables,
    TString cluster,
    TActorSystem* actorSystem
) {
    return THolder<IGraphTransformer>(new TKqpColumnStatisticsRequester(config, typesCtx, tables, cluster, actorSystem));
}

} // end of NKikimr::NKqp

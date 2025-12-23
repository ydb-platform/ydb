#include "kqp_column_statistics_requester.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <yql/essentials/core/yql_statistics.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <yql/essentials/utils/log/log.h>

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

    auto getStatisticsRequestCM = MakeHolder<NStat::TEvStatistics::TEvGetStatistics>();
    getStatisticsRequestCM->Database = Database;
    getStatisticsRequestCM->StatType = NKikimr::NStat::EStatType::COUNT_MIN_SKETCH;

    auto getStatisticsRequestHist = MakeHolder<NStat::TEvStatistics::TEvGetStatistics>();
    getStatisticsRequestHist->Database = Database;
    getStatisticsRequestHist->StatType = NKikimr::NStat::EStatType::EQ_WIDTH_HISTOGRAM;


    for (const auto& [table, columns]: ColumnsByTableName) {
        auto tableMeta = Tables.GetTable(Cluster, table).Metadata;
        auto& columnsMeta = tableMeta->Columns;

        auto pathId = TPathId(tableMeta->PathId.OwnerId(), tableMeta->PathId.TableId());
        for (const auto& column: columns) {
            if (TypesCtx.ColumnStatisticsByTableName.contains(table) && TypesCtx.ColumnStatisticsByTableName[table]->Data.contains(column)) {
                continue;
            }

            if (!columnsMeta.contains(column)) {
                YQL_CLOG(DEBUG, ProviderKikimr) << "Table: " + table + " doesn't contain " + column + " to request for column statistics";
                continue;
            }

            NKikimr::NStat::TRequest req;
            req.ColumnTag = columnsMeta[column].Id;
            req.PathId = pathId;
            getStatisticsRequestCM->StatRequests.push_back(req);
            getStatisticsRequestHist->StatRequests.push_back(req);

            tableMetaByPathId[pathId].TableName = table;
            tableMetaByPathId[pathId].ColumnNameByTag[req.ColumnTag.value()] = column;
        }
    }

    if (getStatisticsRequestCM->StatRequests.empty() && getStatisticsRequestHist->StatRequests.empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

    using TRequest = NStat::TEvStatistics::TEvGetStatistics;
    using TResponse = NStat::TEvStatistics::TEvGetStatisticsResult;

    AsyncReadiness = NewPromise<void>();
    auto promiseCM = NewPromise<TColumnStatisticsResponse>();
    auto promiseHist = NewPromise<TColumnStatisticsResponse>();

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
            if (stat.CountMinSketch.CountMin) {
                columnStatistics.CountMinSketch = std::move(stat.CountMinSketch.CountMin);
            }
            if (stat.EqWidthHistogram.Data) {
                columnStatistics.EqWidthHistogramEstimator = std::make_shared<NKikimr::TEqWidthHistogramEstimator>(stat.EqWidthHistogram.Data);
            }
        }

        promise.SetValue(TColumnStatisticsResponse{.ColumnStatisticsByTableName = std::move(columnStatisticsByTableName)});
    };
    auto statServiceId = NStat::MakeStatServiceID(ActorSystem->NodeId);
    IActor* requestHandlerCM =
        new TActorRequestHandler<TRequest, TResponse, TColumnStatisticsResponse>(statServiceId, getStatisticsRequestCM.Release(), promiseCM, callback);
    ActorSystem
        ->Register(requestHandlerCM, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);

    promiseCM.GetFuture().Subscribe([this](auto result){ ColumnStatisticsResponse = result.ExtractValue(); AsyncReadiness.SetValue(); });

    IActor* requestHandlerHist =
        new TActorRequestHandler<TRequest, TResponse, TColumnStatisticsResponse>(statServiceId, getStatisticsRequestHist.Release(), promiseHist, callback);
    ActorSystem
        ->Register(requestHandlerHist, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);

    promiseHist.GetFuture().Subscribe([this](auto result){ ColumnStatisticsResponse = result.ExtractValue(); AsyncReadiness.SetValue(); });
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

TMaybe<std::pair<TString, TString>> TKqpColumnStatisticsRequester::GetTableAndColumnNames(const TCoMember& member) {
    auto exprNode = TExprBase(member).Ptr();
    if (!KqpTableByExprNode.contains(exprNode) || KqpTableByExprNode[exprNode] == nullptr) {
        return {};
    }

    auto table = TExprBase(KqpTableByExprNode[exprNode]).Cast<TKqpTable>().Path().StringValue();
    auto column = member.Name().StringValue();
    size_t pointPos = column.find('.'); // table.column
    if (pointPos != TString::npos) {
        column = column.substr(pointPos + 1);
    }

    return std::pair{std::move(table), std::move(column)};
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
            if (auto maybeTableAndColumn = GetTableAndColumnNames(item.Member)) {
                const auto& [table, column] = *maybeTableAndColumn;
                ColumnsByTableName[table].insert(std::move(column));
            }
        }

        auto memberEqualities = computer.GetMemberEqualities();
        for (const auto& [lhs, rhs]: memberEqualities) {
            auto maybeLhsTableAndColumn = GetTableAndColumnNames(lhs);
            if (!maybeLhsTableAndColumn) {
                continue;
            }

            auto maybeRhsTableAndColumn = GetTableAndColumnNames(rhs);
            if (!maybeRhsTableAndColumn) {
                continue;
            }

            // const auto& [lhsTable, lhsColumn] = *maybeLhsTableAndColumn;
            // const auto& [rhsTable, rhsColumn] = *maybeRhsTableAndColumn;
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
    const TString& cluster,
    const TString& database,
    TActorSystem* actorSystem
) {
    return THolder<IGraphTransformer>(new TKqpColumnStatisticsRequester(config, typesCtx, tables, cluster, database, actorSystem));
}

} // end of NKikimr::NKqp

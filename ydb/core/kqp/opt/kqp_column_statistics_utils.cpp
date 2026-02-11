#include "kqp_column_statistics_utils.h"

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

// This functions is moved from kqp_op_statistics_requester to be able to use it in other transformers.
void AddStatRequest(TActorSystem* actorSystem, TVector<NThreading::TFuture<TColumnStatisticsResponse>>& futures, TKikimrTablesData& tables,
                    const TString& cluster, const TString& database, TTypeAnnotationContext& typesCtx, const NKikimr::NStat::EStatType type,
                    const THashMap<TString, THashSet<TString>>& columnsByTableName, std::function<bool(const TColumnStatistics&)> alreadyHasStatistics) {
    struct TTableMeta {
        TString TableName;
        THashMap<ui32, TString> ColumnNameByTag;
    };

    THashMap<TPathId, TTableMeta> tableMetaByPathId;
    std::vector<NKikimr::NStat::TRequest> statRequests;
    for (const auto& [table, columns] : columnsByTableName) {
        auto tableMeta = tables.GetTable(cluster, table).Metadata;
        auto& columnsMeta = tableMeta->Columns;

        auto pathId = TPathId(tableMeta->PathId.OwnerId(), tableMeta->PathId.TableId());

        auto statsTableIt = typesCtx.ColumnStatisticsByTableName.find(table);
        for (const auto& column : columns) {
            if (statsTableIt != typesCtx.ColumnStatisticsByTableName.end()) {
                auto statsColumnIt = statsTableIt->second->Data.find(column);
                if (statsColumnIt != statsTableIt->second->Data.end()) {
                    if (alreadyHasStatistics(statsColumnIt->second)) {
                        continue;
                    }
                }
            }

            if (!columnsMeta.contains(column)) {
                YQL_CLOG(DEBUG, ProviderKikimr) << "Table: " + table + " doesn't contain " + column + " to request for column statistics";
                continue;
            }

            NKikimr::NStat::TRequest req;
            req.ColumnTag = columnsMeta[column].Id;
            req.PathId = pathId;
            statRequests.push_back(req);

            tableMetaByPathId[pathId].TableName = table;
            tableMetaByPathId[pathId].ColumnNameByTag[req.ColumnTag.value()] = column;
        }
    }

    if (statRequests.empty()) {
        return;
    }

    auto request = MakeHolder<NStat::TEvStatistics::TEvGetStatistics>();
    request->Database = database;
    request->StatType = type;
    request->StatRequests = std::move(statRequests);

    auto callback = [tableMetaByPathId = std::move(tableMetaByPathId)](NThreading::TPromise<TColumnStatisticsResponse> promise,
                                                                       NStat::TEvStatistics::TEvGetStatisticsResult&& response) mutable {
        if (!response.Success) {
            promise.SetValue(NYql::NCommon::ResultFromError<TColumnStatisticsResponse>("can't get column statistics!"));
            return;
        }

        THashMap<TString, TOptimizerStatistics::TColumnStatMap> columnStatisticsByTableName;

        for (auto&& stat : response.StatResponses) {
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

    using TRequest = NStat::TEvStatistics::TEvGetStatistics;
    using TResponse = NStat::TEvStatistics::TEvGetStatisticsResult;

    auto promise = NThreading::NewPromise<TColumnStatisticsResponse>();

    auto statServiceId = NStat::MakeStatServiceID(actorSystem->NodeId);
    IActor* requestHandler = new TActorRequestHandler<TRequest, TResponse, TColumnStatisticsResponse>(statServiceId, request.Release(), promise, callback);
    actorSystem->Register(requestHandler, TMailboxType::HTSwap, actorSystem->AppData<TAppData>()->UserPoolId);

    futures.push_back(promise.GetFuture());
}
}
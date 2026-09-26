#include "kqp_column_statistics_utils.h"

#include <yql/essentials/core/yql_type_annotation.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

struct TTableMeta {
    TString TableName;
    THashMap<ui32, TString> ColumnNameByTag;
    THashMap<ui32, TString> ColumnTypeByTag;
};

void DispatchStatRequests(TActorSystem* actorSystem, TVector<NThreading::TFuture<TColumnStatisticsResponse>>& futures,
                          TKikimrTablesData& tables, const TString& cluster, const TString& database,
                          const NKikimr::NStat::EStatType type,
                          const TVector<std::pair<TString, TVector<TString>>>& columnTuples) {
    THashMap<TPathId, TTableMeta> tableMetaByPathId;
    std::vector<NKikimr::NStat::TRequest> statRequests;

    for (const auto& [table, columns] : columnTuples) {
        auto tableMeta = tables.GetTable(cluster, table).Metadata;
        if (!tableMeta
            || (tableMeta->Kind != EKikimrTableKind::Datashard
                && tableMeta->Kind != EKikimrTableKind::Olap)) {
            continue;
        }
        auto& columnsMeta = tableMeta->Columns;

        auto pathId = TPathId(tableMeta->PathId.OwnerId(), tableMeta->PathId.TableId());

        std::vector<ui32> columnTags;
        bool allColumnsKnown = true;
        for (const auto& column : columns) {
            if (!columnsMeta.contains(column)) {
                YQL_CLOG(DEBUG, ProviderKikimr) << "Table: " + table + " doesn't contain " + column + " to request for column statistics";
                allColumnsKnown = false;
                break;
            }
            columnTags.push_back(columnsMeta[column].Id);
        }
        if (!allColumnsKnown || columnTags.empty()) {
            continue;
        }

        auto& meta = tableMetaByPathId[pathId];
        meta.TableName = table;
        for (size_t i = 0; i < columnTags.size(); ++i) {
            meta.ColumnNameByTag[columnTags[i]] = columns[i];
            meta.ColumnTypeByTag[columnTags[i]] = columnsMeta[columns[i]].Type;
        }

        NKikimr::NStat::TRequest req;
        req.PathId = pathId;
        if (columnTags.size() == 1) {
            req.ColumnTags = columnTags[0];
        } else {
            req.ColumnTags = std::move(columnTags);
        }
        statRequests.push_back(std::move(req));
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

        THashMap<TString, NYql::TOptimizerStatistics::TColumnStatMap> columnStatisticsByTableName;

        for (auto&& stat : response.StatResponses) {
            auto meta = tableMetaByPathId[stat.Req.PathId];
            if (const auto singleTag = stat.Req.ColumnTags.AsSingle()) {
                auto columnName = meta.ColumnNameByTag[*singleTag];
                auto& columnStatistics = columnStatisticsByTableName[meta.TableName].Data[columnName];
                columnStatistics.Type = meta.ColumnTypeByTag[*singleTag];
                if (stat.CountMinSketch.CountMin) {
                    columnStatistics.CountMinSketch = std::move(stat.CountMinSketch.CountMin);
                }
                if (stat.EqWidthHistogram.Data) {
                    columnStatistics.EqWidthHistogramEstimator = std::make_shared<NKikimr::TEqWidthHistogramEstimator>(stat.EqWidthHistogram.Data);
                }
            } else if (const auto* multiTags = stat.Req.ColumnTags.AsMulti()) {
                TVector<TString> columns;
                TVector<TString> types;
                for (const ui32 columnTag : *multiTags) {
                    columns.push_back(meta.ColumnNameByTag[columnTag]);
                    types.push_back(meta.ColumnTypeByTag[columnTag]);
                }

                auto& multiColumnStatistics = columnStatisticsByTableName[meta.TableName].MultiData[NYql::MakeMultiColumnKey(columns)];
                multiColumnStatistics.Columns = std::move(columns);
                multiColumnStatistics.Types = std::move(types);
                if (stat.EqHeightHistogram.Data) {
                    multiColumnStatistics.EqHeightHistogram = stat.EqHeightHistogram.Data;
                }
                if (stat.CountMinSketch.CountMin) {
                    multiColumnStatistics.CountMinSketch = std::move(stat.CountMinSketch.CountMin);
                }
            } else {
                Y_ENSURE(false, "Expected a column-tagged stat response");
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

} // anonymous namespace

// These functions are moved from kqp_op_statistics_requester to be able to use them in other transformers.
void AddStatRequest(TActorSystem* actorSystem, TVector<NThreading::TFuture<TColumnStatisticsResponse>>& futures, TKikimrTablesData& tables,
                    const TString& cluster, const TString& database, TTypeAnnotationContext& typesCtx, const NKikimr::NStat::EStatType type,
                    const THashMap<TString, THashSet<TString>>& columnsByTableName, std::function<bool(const NYql::TColumnStatistics&)> alreadyHasStatistics) {
    TVector<std::pair<TString, TVector<TString>>> columnTuples;

    for (const auto& [table, columns] : columnsByTableName) {
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

            columnTuples.emplace_back(table, TVector<TString>{column});
        }
    }

    DispatchStatRequests(actorSystem, futures, tables, cluster, database, type, columnTuples);
}

void AddStatRequest(TActorSystem* actorSystem, TVector<NThreading::TFuture<TColumnStatisticsResponse>>& futures, TKikimrTablesData& tables,
                    const TString& cluster, const TString& database, TTypeAnnotationContext& typesCtx, const NKikimr::NStat::EStatType type,
                    const THashMap<TString, THashMap<TString, TVector<TString>>>& columnTuplesByTableName,
                    std::function<bool(const NYql::TMultiColumnStatistics&)> alreadyHasStatistics) {
    TVector<std::pair<TString, TVector<TString>>> columnTuples;

    for (const auto& [table, tuples] : columnTuplesByTableName) {
        auto statsTableIt = typesCtx.ColumnStatisticsByTableName.find(table);
        for (const auto& [tupleKey, columns] : tuples) {
            if (statsTableIt != typesCtx.ColumnStatisticsByTableName.end()) {
                auto statsTupleIt = statsTableIt->second->MultiData.find(tupleKey);
                if (statsTupleIt != statsTableIt->second->MultiData.end()) {
                    if (alreadyHasStatistics(statsTupleIt->second)) {
                        continue;
                    }
                }
            }

            columnTuples.emplace_back(table, columns);
        }
    }

    DispatchStatRequests(actorSystem, futures, tables, cluster, database, type, columnTuples);
}

} // namespace NKikimr::NKqp

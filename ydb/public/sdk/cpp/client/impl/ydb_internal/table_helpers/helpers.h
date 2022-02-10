#pragma once 
 
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>
 
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/query_stats/stats.h>
 
namespace NYdb { 
 
template<typename TFrom> 
inline void PermissionToSchemeEntry(const TFrom& from, TVector<NScheme::TPermissions>* to) { 
    for (const auto& effPerm : from) { 
        to->push_back(NScheme::TPermissions{effPerm.subject(), TVector<TString>()}); 
        for (const auto& permName : effPerm.permission_names()) { 
            to->back().PermissionNames.push_back(permName); 
        } 
    } 
} 
 
inline Ydb::Table::QueryStatsCollection::Mode GetStatsCollectionMode(TMaybe<NTable::ECollectQueryStatsMode> mode) { 
    if (mode) { 
        switch (*mode) { 
            case NTable::ECollectQueryStatsMode::None: 
                return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE; 
            case NTable::ECollectQueryStatsMode::Basic: 
                return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC; 
            case NTable::ECollectQueryStatsMode::Full: 
                return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL; 
            default: 
                break; 
        } 
    } 
 
    return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_UNSPECIFIED; 
} 
 
} // namespace NYdb 

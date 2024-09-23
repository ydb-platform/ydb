#pragma once

#include <ydb/core/external_sources/hive_metastore/hive_metastore_native/gen-cpp/ThriftHiveMetastore.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/threading/future/core/future.h>

namespace NKikimr::NExternalSource {

struct TEvHiveMetastore {
    // Event ids.
    enum EEv : ui32 {
        EvGetTable = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvGetStatistics,
        EvGetPartitions,

        EvHiveGetTableResult,
        EvHiveGetTableStatisticsResult,
        EvHiveGetPartitionsResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents:ES_PRIVATE)");

    struct TTable {
        std::vector<Ydb::Column> Columns;
        TString Location;
        TString Format;
        TString Compression;
        std::vector<TString> PartitionedBy;
    };

    struct TEvGetTable: NActors::TEventLocal<TEvGetTable, EvGetTable> {
        TString DatbaseName;
        TString TableName;
        NThreading::TPromise<TTable> Promise;
    };

    struct TStatistics {
        TMaybe<int64_t> Rows;
        TMaybe<int64_t> Size;
    };

    struct TEvGetStatistics: NActors::TEventLocal<TEvGetStatistics, EvGetStatistics> {
        TString DatbaseName;
        TString TableName;
        std::vector<std::string> Columns;
        NThreading::TPromise<TStatistics> Promise;
    };

    struct TPartitions {
        struct TPartition {
            TString Location;
            std::vector<TString> Values;
        };
        std::vector<TPartition> Partitions;
    };

    struct TEvGetPartitions: NActors::TEventLocal<TEvGetPartitions, EvGetPartitions> {
        TString DatbaseName;
        TString TableName;
        NYql::NConnector::NApi::TPredicate Predicate;
        NThreading::TPromise<TPartitions> Promise;
    };

    struct TEvHiveGetTableResult: NActors::TEventLocal<TEvHiveGetTableResult, EvHiveGetTableResult> {
        Apache::Hadoop::Hive::Table Table;
        NYql::TIssues Issues;
    };

    struct TEvHiveGetTableStatisticsResult: NActors::TEventLocal<TEvHiveGetTableStatisticsResult, EvHiveGetTableStatisticsResult> {
        Apache::Hadoop::Hive::TableStatsResult Statistics;
        NYql::TIssues Issues;
    };

    struct TEvHiveGetPartitionsResult: NActors::TEventLocal<TEvHiveGetPartitionsResult, EvHiveGetPartitionsResult> {
        std::vector<Apache::Hadoop::Hive::Partition> Partitions;
        NYql::TIssues Issues;
    };
};

}

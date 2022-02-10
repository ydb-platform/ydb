#pragma once 
 
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/protos/ydb_query_stats.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
 
#include <ydb/public/sdk/cpp/client/ydb_export/export.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

namespace NYdb { 
 
class TResultSet;
class TValue;
class TType;

namespace NTable {
class TQueryStats;
class TTableDescription;
class TIndexDescription;
}

//! Provides access to raw protobuf values of YDB API entities. It is not recommended to use this 
//! class in client applications as it add dependency on API protobuf format which is subject to 
//! change. Use functionality provided by YDB SDK classes. 
class TProtoAccessor { 
public: 
    static const Ydb::Type& GetProto(const TType& type); 
    static const Ydb::Value& GetProto(const TValue& value); 
    static const Ydb::ResultSet& GetProto(const TResultSet& resultSet); 
    static const Ydb::TableStats::QueryStats& GetProto(const NTable::TQueryStats& queryStats);
    static const Ydb::Table::DescribeTableResult& GetProto(const NTable::TTableDescription& tableDescription);
    static const Ydb::PersQueue::V1::DescribeTopicResult& GetProto(const NYdb::NPersQueue::TDescribeTopicResult& topicDescription);

    static NTable::TQueryStats FromProto(const Ydb::TableStats::QueryStats& queryStats);
    static NTable::TTableDescription FromProto(const Ydb::Table::CreateTableRequest& request);
    static NTable::TIndexDescription FromProto(const Ydb::Table::TableIndex& tableIndex);
    static NTable::TIndexDescription FromProto(const Ydb::Table::TableIndexDescription& tableIndexDesc);

    static Ydb::Table::ValueSinceUnixEpochModeSettings::Unit GetProto(NTable::TValueSinceUnixEpochModeSettings::EUnit value);
    static NTable::TValueSinceUnixEpochModeSettings::EUnit FromProto(Ydb::Table::ValueSinceUnixEpochModeSettings::Unit value);

    // exports & imports
    template <typename TProtoSettings> static typename TProtoSettings::Scheme GetProto(ES3Scheme value);
    template <typename TProtoSettings> static ES3Scheme FromProto(typename TProtoSettings::Scheme value);
    static Ydb::Export::ExportToS3Settings::StorageClass GetProto(NExport::TExportToS3Settings::EStorageClass value);
    static NExport::TExportToS3Settings::EStorageClass FromProto(Ydb::Export::ExportToS3Settings::StorageClass value);
    static NExport::EExportProgress FromProto(Ydb::Export::ExportProgress::Progress value);
    static NImport::EImportProgress FromProto(Ydb::Import::ImportProgress::Progress value);
}; 
 
} // namespace NYdb 

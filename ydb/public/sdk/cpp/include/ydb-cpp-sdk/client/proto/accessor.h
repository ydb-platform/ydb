#pragma once

#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/api/protos/draft/ydb_view.pb.h>
#include <ydb/public/api/protos/ydb_coordination.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_query_stats.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb-cpp-sdk/client/draft/ydb_view.h>
#include <ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb-cpp-sdk/client/export/export.h>
#include <ydb-cpp-sdk/client/import/import.h>
#include <ydb-cpp-sdk/client/monitoring/monitoring.h>
#include <ydb-cpp-sdk/client/table/table.h>
#include <ydb-cpp-sdk/client/topic/client.h>

namespace NYdb::inline V3 {

class TResultSet;
class TValue;
class TType;
class TParams;

namespace NTable {
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
    static const ::google::protobuf::Map<TStringType, Ydb::TypedValue>& GetProtoMap(const TParams& params);
    static ::google::protobuf::Map<TStringType, Ydb::TypedValue>* GetProtoMapPtr(TParams& params);
    static const Ydb::TableStats::QueryStats& GetProto(const NTable::TQueryStats& queryStats);
    static const Ydb::Table::DescribeTableResult& GetProto(const NTable::TTableDescription& tableDescription);
    static const Ydb::Table::DescribeExternalDataSourceResult& GetProto(const NTable::TExternalDataSourceDescription&);
    static const Ydb::Table::DescribeExternalTableResult& GetProto(const NTable::TExternalTableDescription&);
    static const Ydb::Topic::DescribeTopicResult& GetProto(const NYdb::NTopic::TTopicDescription& topicDescription);
    static const Ydb::Topic::DescribeConsumerResult& GetProto(const NYdb::NTopic::TConsumerDescription& consumerDescription);
    static const Ydb::Monitoring::SelfCheckResult& GetProto(const NYdb::NMonitoring::TSelfCheckResult& selfCheckResult);
    static const Ydb::Coordination::DescribeNodeResult& GetProto(const NYdb::NCoordination::TNodeDescription &describeNodeResult);
    static const Ydb::Replication::DescribeReplicationResult& GetProto(const NYdb::NReplication::TDescribeReplicationResult& desc);
    static const Ydb::View::DescribeViewResult& GetProto(const NYdb::NView::TDescribeViewResult& desc);

    static NTable::TQueryStats FromProto(const Ydb::TableStats::QueryStats& queryStats);
    static NTable::TTableDescription FromProto(const Ydb::Table::CreateTableRequest& request);
    static NTable::TIndexDescription FromProto(const Ydb::Table::TableIndex& tableIndex);
    static NTable::TIndexDescription FromProto(const Ydb::Table::TableIndexDescription& tableIndexDesc);

    static NTable::TChangefeedDescription FromProto(const Ydb::Table::Changefeed& changefeed);
    static NTable::TChangefeedDescription FromProto(const Ydb::Table::ChangefeedDescription& changefeed);

    static Ydb::Table::ValueSinceUnixEpochModeSettings::Unit GetProto(NTable::TValueSinceUnixEpochModeSettings::EUnit value);
    static NTable::TValueSinceUnixEpochModeSettings::EUnit FromProto(Ydb::Table::ValueSinceUnixEpochModeSettings::Unit value);

    static Ydb::Topic::MeteringMode GetProto(NTopic::EMeteringMode mode);
    static NTopic::EMeteringMode FromProto(Ydb::Topic::MeteringMode mode);

    // exports & imports
    template <typename TProtoSettings> static typename TProtoSettings::Scheme GetProto(ES3Scheme value);
    template <typename TProtoSettings> static ES3Scheme FromProto(typename TProtoSettings::Scheme value);
    static Ydb::Export::ExportToS3Settings::StorageClass GetProto(NExport::TExportToS3Settings::EStorageClass value);
    static NExport::TExportToS3Settings::EStorageClass FromProto(Ydb::Export::ExportToS3Settings::StorageClass value);
    static NExport::EExportProgress FromProto(Ydb::Export::ExportProgress::Progress value);
    static NImport::EImportProgress FromProto(Ydb::Import::ImportProgress::Progress value);
};

} // namespace NYdb

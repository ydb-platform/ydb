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
    static const NYdbProtos::Type& GetProto(const TType& type);
    static const NYdbProtos::Value& GetProto(const TValue& value);
    static const NYdbProtos::ResultSet& GetProto(const TResultSet& resultSet);
    static const ::google::protobuf::Map<TStringType, NYdbProtos::TypedValue>& GetProtoMap(const TParams& params);
    static ::google::protobuf::Map<TStringType, NYdbProtos::TypedValue>* GetProtoMapPtr(TParams& params);
    static const NYdbProtos::TableStats::QueryStats& GetProto(const NTable::TQueryStats& queryStats);
    static const NYdbProtos::Table::DescribeTableResult& GetProto(const NTable::TTableDescription& tableDescription);
    static const NYdbProtos::Table::DescribeExternalDataSourceResult& GetProto(const NTable::TExternalDataSourceDescription&);
    static const NYdbProtos::Table::DescribeExternalTableResult& GetProto(const NTable::TExternalTableDescription&);
    static const NYdbProtos::Topic::DescribeTopicResult& GetProto(const NYdb::NTopic::TTopicDescription& topicDescription);
    static const NYdbProtos::Topic::DescribeConsumerResult& GetProto(const NYdb::NTopic::TConsumerDescription& consumerDescription);
    static const NYdbProtos::Monitoring::SelfCheckResult& GetProto(const NYdb::NMonitoring::TSelfCheckResult& selfCheckResult);
    static const NYdbProtos::Coordination::DescribeNodeResult& GetProto(const NYdb::NCoordination::TNodeDescription &describeNodeResult);
    static const NYdbProtos::Replication::DescribeReplicationResult& GetProto(const NYdb::NReplication::TDescribeReplicationResult& desc);
    static const NYdbProtos::View::DescribeViewResult& GetProto(const NYdb::NView::TDescribeViewResult& desc);

    static NTable::TQueryStats FromProto(const NYdbProtos::TableStats::QueryStats& queryStats);
    static NTable::TTableDescription FromProto(const NYdbProtos::Table::CreateTableRequest& request);
    static NTable::TIndexDescription FromProto(const NYdbProtos::Table::TableIndex& tableIndex);
    static NTable::TIndexDescription FromProto(const NYdbProtos::Table::TableIndexDescription& tableIndexDesc);

    static NTable::TChangefeedDescription FromProto(const NYdbProtos::Table::Changefeed& changefeed);
    static NTable::TChangefeedDescription FromProto(const NYdbProtos::Table::ChangefeedDescription& changefeed);

    static NYdbProtos::Table::ValueSinceUnixEpochModeSettings::Unit GetProto(NTable::TValueSinceUnixEpochModeSettings::EUnit value);
    static NTable::TValueSinceUnixEpochModeSettings::EUnit FromProto(NYdbProtos::Table::ValueSinceUnixEpochModeSettings::Unit value);

    static NYdbProtos::Topic::MeteringMode GetProto(NTopic::EMeteringMode mode);
    static NTopic::EMeteringMode FromProto(NYdbProtos::Topic::MeteringMode mode);

    // exports & imports
    template <typename TProtoSettings> static typename TProtoSettings::Scheme GetProto(ES3Scheme value);
    template <typename TProtoSettings> static ES3Scheme FromProto(typename TProtoSettings::Scheme value);
    static NYdbProtos::Export::ExportToS3Settings::StorageClass GetProto(NExport::TExportToS3Settings::EStorageClass value);
    static NExport::TExportToS3Settings::EStorageClass FromProto(NYdbProtos::Export::ExportToS3Settings::StorageClass value);
    static NExport::EExportProgress FromProto(NYdbProtos::Export::ExportProgress::Progress value);
    static NImport::EImportProgress FromProto(NYdbProtos::Import::ImportProgress::Progress value);
};

} // namespace NYdb

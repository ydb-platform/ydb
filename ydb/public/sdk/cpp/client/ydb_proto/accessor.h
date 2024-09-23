#pragma once

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/api/protos/ydb_coordination.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_query_stats.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/client/ydb_coordination/coordination.h>
#include <ydb/public/sdk/cpp/client/ydb_export/export.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_monitoring/monitoring.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NYdb {

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
    static const ::google::protobuf::Map<TString, Ydb::TypedValue>& GetProtoMap(const TParams& params);
    static ::google::protobuf::Map<TString, Ydb::TypedValue>* GetProtoMapPtr(TParams& params);
    static const Ydb::TableStats::QueryStats& GetProto(const NTable::TQueryStats& queryStats);
    static const Ydb::Table::DescribeTableResult& GetProto(const NTable::TTableDescription& tableDescription);
    static const Ydb::Topic::DescribeTopicResult& GetProto(const NYdb::NTopic::TTopicDescription& topicDescription);
    static const Ydb::Topic::DescribeConsumerResult& GetProto(const NYdb::NTopic::TConsumerDescription& consumerDescription);
    static const Ydb::Monitoring::SelfCheckResult& GetProto(const NYdb::NMonitoring::TSelfCheckResult& selfCheckResult);
    static const Ydb::Coordination::DescribeNodeResult& GetProto(const NYdb::NCoordination::TNodeDescription &describeNodeResult);
    static const Ydb::Replication::DescribeReplicationResult& GetProto(const NYdb::NReplication::TDescribeReplicationResult& desc);

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

#include "schemeshard_scheme_builders.h"
#include "schemeshard_export_helpers.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/sys_view/show_create/formatters/create_table_formatter.h>
#include <ydb/core/ydb_convert/external_data_source_description.h>
#include <ydb/core/ydb_convert/external_table_description.h>
#include <ydb/core/ydb_convert/replication_description.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/topic_description.h>

#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/lib/ydb_cli/dump/util/external_data_source_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/external_table_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/replication_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <exception>

#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {
namespace {

NSysView::TFormatResult FormatCreateTable(const TExportTableSchemeContext& context) {
    const auto& pathDescription = context.PathDescription;
    const auto& tableName = pathDescription.GetSelf().GetName();
    Y_ENSURE(tableName && context.SourcePath, "Missing original table path for SQL backup");

    NSysView::TCreateTableFormatter formatter;
    if (pathDescription.HasColumnTableDescription()) {
        const auto& description = pathDescription.GetColumnTableDescription();
        Y_ENSURE(description.GetSchema().ColumnsSize(), "Column table description has no columns");
        return formatter.Format(tableName, context.SourcePath, description, false,
            AppData()->FeatureFlags.GetEnableLocalIndexAsSchemeObject());
    }

    Y_ENSURE(pathDescription.HasTable(), "Missing table description for SQL backup");
    const auto& description = pathDescription.GetTable();
    const auto& topics = context.ChangefeedUnderlyingTopics.GetChangefeedUnderlyingTopics();
    Y_ENSURE(description.GetCdcStreams().size() == topics.size(),
        "Number of changefeeds does not match backup topic descriptions");

    THashMap<TString, THolder<NKikimrSchemeOp::TPersQueueGroupDescription>> persQueues;
    for (int i = 0; i < description.GetCdcStreams().size(); ++i) {
        const auto topicPath = JoinPath({tableName, description.GetCdcStreams(i).GetName(), "streamImpl"});
        persQueues.emplace(topicPath,
            MakeHolder<NKikimrSchemeOp::TPersQueueGroupDescription>(topics.Get(i).GetPersQueueGroup()));
    }

    THashMap<TPathId, THolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>> sequences;
    for (const auto& sequence : description.GetSequences()) {
        const auto pathId = TPathId::FromProto(sequence.GetPathId());
        auto result = MakeHolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>(pathId);
        result->StartValue = sequence.GetStartValue();
        result->Increment = sequence.GetIncrement();
        result->NextValue = sequence.HasSetVal() ? sequence.GetSetVal().GetNextValue() : sequence.GetStartValue();
        result->NextUsed = sequence.GetSetVal().GetNextUsed();
        sequences.emplace(pathId, std::move(result));
    }

    return formatter.Format(tableName, context.SourcePath, description, false, persQueues, sequences);
}

} // namespace

bool BuildCreateTableScheme(
    const TExportTableSchemeContext& context,
    TString& scheme,
    TString& error)
{
    scheme.clear();
    error.clear();

    try {
        auto result = FormatCreateTable(context);
        if (!result.IsSuccess()) {
            error = result.GetError();
            return false;
        }

        scheme = result.ExtractOut();
        if (!scheme) {
            error = "CREATE TABLE query is empty";
            return false;
        }

        return true;
    } catch (const NSysView::TFormatFail& exception) {
        error = exception.Error;
    } catch (const std::exception& exception) {
        error = exception.what();
    }

    return false;
}

bool BuildViewScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& database,
    TString& error)
{
    const auto& pathDesc = describeResult.GetPathDescription();
    if (!pathDesc.HasViewDescription()) {
        error = "Path description does not contain a description of View";
        return false;
    }

    const auto& viewDesc = pathDesc.GetViewDescription();
    NYql::TIssues issues;
    scheme = NYdb::NDump::BuildCreateViewQuery(
        viewDesc.GetName(),
        describeResult.GetPath(),
        viewDesc.GetQueryText(),
        database,
        database,
        issues
    );

    if (!scheme) {
        error = issues.ToString();
        return false;
    }
    return true;
}

bool BuildTopicScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    TString& error)
{
    const auto& pathDesc = describeResult.GetPathDescription();
    if (!pathDesc.HasPersQueueGroup()) {
        error = "Path description does not contain a description of PersQueueGroup";
        return false;
    }

    Ydb::Topic::DescribeTopicResult descTopicResult;
    Ydb::StatusIds::StatusCode status;
    if (!FillTopicDescription(descTopicResult, pathDesc.GetPersQueueGroup(), pathDesc.GetSelf(), Nothing(), status, error)) {
        return false;
    }

    Ydb::Topic::CreateTopicRequest request;
    NYdb::NTopic::TTopicDescription(std::move(descTopicResult)).SerializeTo(request);

    return google::protobuf::TextFormat::PrintToString(request, &scheme);
}

bool BuildReplicationScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& database,
    TString& error)
{
    const auto& pathDesc = describeResult.GetPathDescription();
    if (!pathDesc.HasReplicationDescription()) {
        error = "Path description does not contain a description of Async Replication";
        return false;
    }

    const auto& replicationDesc = pathDesc.GetReplicationDescription();
    Ydb::Replication::DescribeReplicationResult replicationDescResult;
    Ydb::StatusIds::StatusCode status;

    if (!FillReplicationDescription(replicationDescResult, replicationDesc, pathDesc.GetSelf(), status, error)) {
        return false;
    }

    scheme = NYdb::NDump::BuildCreateReplicationQuery(
        database, database, replicationDesc.GetName(),
        NYdb::NReplication::TReplicationDescription(replicationDescResult));

    return true;
}

bool BuildTransferScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& database,
    TString& error)
{
    const auto& pathDesc = describeResult.GetPathDescription();
    if (!pathDesc.HasReplicationDescription()) {
        error = "Path description does not contain a description of transfer";
        return false;
    }

    const auto& replicationDesc = pathDesc.GetReplicationDescription();
    Ydb::Replication::DescribeTransferResult transferDescResult;
    Ydb::StatusIds::StatusCode status;

    if (!FillTransferDescription(transferDescResult, replicationDesc, pathDesc.GetSelf(), status, error)) {
        return false;
    }

    scheme = NYdb::NDump::BuildCreateTransferQuery(
        database, database, replicationDesc.GetName(),
        NYdb::NReplication::TTransferDescription(transferDescResult));

    return true;
}

bool BuildExternalDataSourceScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& database,
    TString& error)
{
    const auto& pathDesc = describeResult.GetPathDescription();
    if (!pathDesc.HasExternalDataSourceDescription()) {
        error = "Path description does not contain a description of external data source";
        return false;
    }

    const auto& dataSourceDesc = pathDesc.GetExternalDataSourceDescription();
    Ydb::Table::DescribeExternalDataSourceResult dataSourceDescResult;

    FillExternalDataSourceDescription(dataSourceDescResult, dataSourceDesc, pathDesc.GetSelf());
    dataSourceDescResult.mutable_properties()->erase("REFERENCES");

    scheme = NYdb::NDump::BuildCreateExternalDataSourceQuery(dataSourceDescResult, database);

    return true;
}

bool BuildExternalTableScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& database,
    TString& error)
{
    const auto& pathDesc = describeResult.GetPathDescription();
    if (!pathDesc.HasExternalTableDescription()) {
        error = "Path description does not contain a description of external table";
        return false;
    }

    const auto& externalTableDesc = pathDesc.GetExternalTableDescription();
    Ydb::Table::DescribeExternalTableResult externalTableDescResult;
    Ydb::StatusIds::StatusCode status;

    if (!FillExternalTableDescription(externalTableDescResult, externalTableDesc, pathDesc.GetSelf(), status, error)) {
        return false;
    }

    scheme = NYdb::NDump::BuildCreateExternalTableQuery(database, database, externalTableDescResult);

    return true;
}

bool BuildSysViewScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    TString& error)
{
    Ydb::Table::DescribeSystemViewResult describeSysViewResult;
    Ydb::StatusIds_StatusCode status;

    const auto& pathDescription = describeResult.GetPathDescription();
    if (!FillSysViewDescription(describeSysViewResult, pathDescription, status, error)) {
        return false;
    }

    describeSysViewResult.clear_self();

    google::protobuf::TextFormat::PrintToString(describeSysViewResult, &scheme);
    return true;
}

bool BuildScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& databaseRoot,
    TString& error)
{
    const auto pathType = GetPathType(describeResult);

    switch (pathType) {
        case NKikimrSchemeOp::EPathTypeView:
            return BuildViewScheme(describeResult, scheme, databaseRoot, error);
        case NKikimrSchemeOp::EPathTypePersQueueGroup:
            return BuildTopicScheme(describeResult, scheme, error);
        case NKikimrSchemeOp::EPathTypeReplication:
            return BuildReplicationScheme(describeResult, scheme, databaseRoot, error);
        case NKikimrSchemeOp::EPathTypeTransfer:
            return BuildTransferScheme(describeResult, scheme, databaseRoot, error);
        case NKikimrSchemeOp::EPathTypeExternalDataSource:
            return BuildExternalDataSourceScheme(describeResult, scheme, databaseRoot, error);
        case NKikimrSchemeOp::EPathTypeExternalTable:
            return BuildExternalTableScheme(describeResult, scheme, databaseRoot, error);
        case NKikimrSchemeOp::EPathTypeSysView:
            return BuildSysViewScheme(describeResult, scheme, error);
        default:
            error = TStringBuilder() << "unsupported path type: " << pathType;
            return false;
    }
}

} // namespace NKikimr::NSchemeShard

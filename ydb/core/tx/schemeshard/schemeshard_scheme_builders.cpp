#include "schemeshard_scheme_builders.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/core/ydb_convert/replication_description.h>

#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/lib/ydb_cli/dump/util/replication_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {

bool BuildViewScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& database,
    const TString& backupRoot,
    TString& error) {

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
        backupRoot,
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
    TString& error) {

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

    request.clear_attributes();

    return google::protobuf::TextFormat::PrintToString(request, &scheme);
}

bool BuildReplicationScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& database,
    const TString& backupRoot,
    TString& error) {

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
        database, backupRoot, replicationDesc.GetName(),
        NYdb::NReplication::TReplicationDescription(replicationDescResult));

    return true;
}

bool BuildScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& databaseRoot,
    TString& error) {

    const auto pathType = GetPathType(describeResult);

    switch (pathType) {
        case NKikimrSchemeOp::EPathTypeView:
            return BuildViewScheme(describeResult, scheme, databaseRoot, databaseRoot, error);
        case NKikimrSchemeOp::EPathTypePersQueueGroup:
            return BuildTopicScheme(describeResult, scheme, error);
        case NKikimrSchemeOp::EPathTypeReplication:
            return BuildReplicationScheme(describeResult, scheme, databaseRoot, databaseRoot, error);
        default:
            error = TStringBuilder() << "unsupported path type: " << pathType;
            return false;
    }
}

NKikimrSchemeOp::EPathType GetPathType(const NKikimrScheme::TEvDescribeSchemeResult& describeResult) {
    return describeResult.GetPathDescription().GetSelf().GetPathType();
}

} // namespace NKikimr::NSchemeShard

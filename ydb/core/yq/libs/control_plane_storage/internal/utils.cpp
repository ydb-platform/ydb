#include "utils.h"

#include <ydb/core/metering/bill_record.h>
#include <ydb/core/metering/metering.h>

namespace NYq {

NYql::TIssues ValidateWriteResultData(const TString& resultId, const Ydb::ResultSet& resultSet, const TInstant& deadline, const TDuration& ttl)
{
    NYql::TIssues issues;
    if (!resultId) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "result id is not specified"));
    }

    if (resultSet.rows().size() == 0) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "result set rows count is empty"));
    }

    const auto hardLimit = TInstant::Now() + ttl;
    if (deadline > hardLimit) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "deadline " + deadline.ToString() + " must be less than " + hardLimit.ToString()));
    }

    return issues;
}

NYql::TIssues ValidateGetTask(const TString& owner, const TString& hostName)
{
    NYql::TIssues issues;
    if (!owner) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "owner is not specified"));
    }

    if (!hostName) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "host name is not specified"));
    }

    return issues;
}

NYql::TIssues ValidatePingTask(const TString& scope, const TString& queryId, const TString& owner, const TInstant& deadline, const TDuration& ttl)
{
    NYql::TIssues issues;
    if (!scope) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "scope is not specified"));
    }

    if (!queryId) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "query id is not specified"));
    }

    if (!owner) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "owner is not specified"));
    }

    const auto hardLimit = TInstant::Now() + ttl;
    if (deadline > hardLimit) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "deadline " + deadline.ToString() + " must be less than " + hardLimit.ToString()));
    }

    return issues;
}

NYql::TIssues ValidateNodesHealthCheck(
    const TString& tenant,
    const TString& instanceId,
    const TString& hostName
    )
{
    NYql::TIssues issues;
    if (!tenant) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "tenant is not specified"));
    }

    if (!instanceId) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "instance id is not specified"));
    }

    if (!hostName) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "hostName is not specified"));
    }

    return issues;
}

NYql::TIssues ValidateCreateOrDeleteRateLimiterResource(const TString& queryId, const TString& scope, const TString& tenant, const TString& owner)
{
    NYql::TIssues issues;
    if (!queryId) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "query id is not specified"));
    }

    if (!scope) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "scope is not specified"));
    }

    if (!tenant) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "tenant is not specified"));
    }

    if (!owner) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "owner is not specified"));
    }

    return issues;
}

std::vector<TString> GetMeteringRecords(const TString& statistics, const TString& queryId, const TString& scope, const TString& sourceId) {

    std::vector<TString> result;
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue stat;

    if (NJson::ReadJsonTree(statistics, &jsonConfig, &stat)) {
        ui64 ingress = 0;
        ui64 egress = 0;
        for (const auto& p : stat.GetMap()) {
            if (p.first.StartsWith("Graph=") || p.first.StartsWith("Precompute=")) {
                if (auto* ingressNode = p.second.GetValueByPath("TaskRunner.Stage=Total.IngressS3SourceBytes.count")) {
                    ingress += ingressNode->GetInteger();
                }
                if (auto* egressNode = p.second.GetValueByPath("TaskRunner.Stage=Total.EgressS3SinkBytes.count")) {
                    egress += egressNode->GetInteger();
                }
            }
        }
        if (ingress) {
            auto now = Now();
            result.emplace_back(TBillRecord()
                .Id(queryId + "_osi")
                .Schema("yq.object_storage.ingress")
                .FolderId(TScope(scope).ParseFolder())
                .SourceWt(now)
                .SourceId(sourceId)
                .Usage(TBillRecord::TUsage()
                    .Type(TBillRecord::TUsage::EType::Delta)
                    .Unit(TBillRecord::TUsage::EUnit::Byte)
                    .Quantity(ingress)
                    .Start(now)
                    .Finish(now)
                )
                .ToString()
            );
        }
        if (egress) {
            auto now = Now();
            result.emplace_back(TBillRecord()
                .Id(queryId + "_ose")
                .Schema("yq.object_storage.egress")
                .FolderId(TScope(scope).ParseFolder())
                .SourceWt(now)
                .SourceId(sourceId)
                .Usage(TBillRecord::TUsage()
                    .Type(TBillRecord::TUsage::EType::Delta)
                    .Unit(TBillRecord::TUsage::EUnit::Byte)
                    .Quantity(egress)
                    .Start(now)
                    .Finish(now)
                )
                .ToString()
            );
        }
    }

    return result;
}

};

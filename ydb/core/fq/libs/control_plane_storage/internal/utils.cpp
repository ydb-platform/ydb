#include "utils.h"

#include <library/cpp/json/yson/json2yson.h>

#include <ydb/core/metering/bill_record.h>
#include <ydb/core/metering/metering.h>
#include <ydb/public/lib/fq/scope.h>

namespace NFq {

using NYdb::NFq::TScope;

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

std::vector<TString> GetMeteringRecords(const TString& statistics, bool billable, const TString& jobId, const TString& scope, const TString& sourceId) {

    std::vector<TString> result;
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue stat;

    ui64 ingress = 0;

    if (billable) {
        if (NJson::ReadJsonTree(statistics, &jsonConfig, &stat)) {
            for (const auto& p : stat.GetMap()) {
                if (p.first.StartsWith("Graph=") || p.first.StartsWith("Precompute=")) {
                    if (auto* ingressNode = p.second.GetValueByPath("TaskRunner.Stage=Total.IngressS3SourceBytes.count")) {
                        ingress += ingressNode->GetInteger();
                    }
                }
            }
        }
    }

    if (ingress) {
        auto ingressMBytes = (ingress + 1_MB - 1) >> 20; // round up to 1 MB boundary
        if (ingressMBytes < 10) {
            ingressMBytes = 10;
        }

        auto now = Now();
        result.emplace_back(TBillRecord()
            .Id(jobId + "_i")
            .Schema("yq.ingress.v1")
            .FolderId(TScope(scope).ParseFolder())
            .SourceWt(now)
            .SourceId(sourceId)
            .Usage(TBillRecord::TUsage()
                .Type(TBillRecord::TUsage::EType::Delta)
                .Unit(TBillRecord::TUsage::EUnit::MByte)
                .Quantity(ingressMBytes)
                .Start(now)
                .Finish(now)
            )
            .ToString()
        );
    }

    return result;
}

void RemapValue(NYson::TYsonWriter& writer, const NJson::TJsonValue& node, const TString& key) {
    ui64 value = 0;
    if (auto* keyNode = node.GetValueByPath(key)) {
        value = keyNode->GetInteger();
    }
    writer.OnKeyedItem(key);
    writer.OnInt64Scalar(value);
}

void RemapNode(NYson::TYsonWriter& writer, const NJson::TJsonValue& node, const TString& path, const TString& key) {
    if (auto* subNode = node.GetValueByPath(path)) {
        writer.OnKeyedItem(key);
        writer.OnBeginMap();
            RemapValue(writer, *subNode, "sum");
            RemapValue(writer, *subNode, "count");
            RemapValue(writer, *subNode, "avg");
            RemapValue(writer, *subNode, "max");
            RemapValue(writer, *subNode, "min");
        writer.OnEndMap();
    }
}

TString GetPrettyStatistics(const TString& statistics) {
    TStringStream out;
    NYson::TYsonWriter writer(&out);
    writer.OnBeginMap();
    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue stat;
    if (NJson::ReadJsonTree(statistics, &jsonConfig, &stat)) {
        for (const auto& p : stat.GetMap()) {
            if (p.first.StartsWith("Graph=") || p.first.StartsWith("Precompute=")) {
                writer.OnKeyedItem(p.first);
                writer.OnBeginMap();
                    RemapNode(writer, p.second, "StagesCount", "StagesCount");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.TasksCount", "TasksCount");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.BuildCpuTimeUs", "BuildCpuTimeUs");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.ComputeCpuTimeUs", "ComputeCpuTimeUs");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.CpuTimeUs", "CpuTimeUs");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.SourceCpuTimeUs", "SourceCpuTimeUs");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.IngressS3SourceBytes", "IngressObjectStorageBytes");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.EgressS3SinkBytes", "EgressObjectStorageBytes");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.IngressPqSourceBytes", "IngressStreamBytes");
                    RemapNode(writer, p.second, "TaskRunner.Stage=Total.EgressPqSinkBytes", "EgressStreamBytes");
                    RemapNode(writer, p.second, "TaskRunner.Source=0.Stage=Total.RowsIn", "IngressRows");
                writer.OnEndMap();
            }
        }
    }
    writer.OnEndMap();
    return NJson2Yson::ConvertYson2Json(out.Str());
}

};

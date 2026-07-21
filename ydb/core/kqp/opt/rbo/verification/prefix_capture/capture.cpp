#include "capture.h"

#include <library/cpp/json/json_writer.h>

#include <util/generic/yexception.h>

#include <regex>

namespace NKikimr::NKqp::NRBOPrefixCapture {
namespace {

constexpr TStringBuf Protocol = "ydb-rbo-rule-prefix-capture-v1";
constexpr TStringBuf InitialSnapshotName = "initial.json";
constexpr TStringBuf PrefixSnapshotName = "prefix.json";
constexpr TStringBuf FinalSnapshotName = "final.json";

void ValidateApplications(
    const TVector<TRBORuleApplicationV1>& applications,
    ui64 expectedCount)
{
    if (applications.size() != expectedCount) {
        ythrow yexception()
            << "capture has " << applications.size()
            << " committed applications; expected " << expectedCount;
    }
    for (ui64 index = 0; index < applications.size(); ++index) {
        const auto& application = applications[index];
        if (application.Ordinal != index + 1 ||
            application.StageName.empty() ||
            application.RuleName.empty())
        {
            ythrow yexception()
                << "committed application " << index + 1
                << " is not contiguous or has an empty stage/rule";
        }
    }
}

void ValidateSnapshotResult(
    const TRBOSemanticSnapshotBoundaryResultV1& result,
    TStringBuf boundary)
{
    if (result.IsSupported() == result.Json.empty()) {
        ythrow yexception()
            << boundary
            << " snapshot must have exactly one of JSON or an unsupported reason";
    }
}

void ValidateOutput(const TCaptureOutput& capture) {
    if (capture.RequestedOrdinal == 0) {
        ythrow yexception() << "requested rule-application ordinal must be positive";
    }
    if (capture.InitialSnapshot.empty()) {
        ythrow yexception() << "initial snapshot is empty";
    }
    if (StatusName(capture.Status) == "UNKNOWN") {
        ythrow yexception() << "capture has an unknown status";
    }

    const bool prefix =
        capture.Status == ECaptureStatus::PrefixCaptured ||
        capture.Status == ECaptureStatus::PrefixUnsupported;
    const ui64 expectedApplications = prefix
        ? capture.RequestedOrdinal
        : capture.Applications.size();
    ValidateApplications(capture.Applications, expectedApplications);
    if (!prefix && capture.Applications.size() >= capture.RequestedOrdinal) {
        ythrow yexception()
            << "completed optimizer reached the requested application ordinal";
    }

    const bool supported =
        capture.Status == ECaptureStatus::PrefixCaptured ||
        capture.Status == ECaptureStatus::OptimizerComplete;
    if (supported == capture.CandidateSnapshot.empty()) {
        ythrow yexception()
            << StatusName(capture.Status)
            << " must have exactly one candidate snapshot";
    }
    if (supported != capture.UnsupportedReason.empty()) {
        ythrow yexception()
            << StatusName(capture.Status)
            << " has inconsistent unsupported diagnostics";
    }
}

} // namespace

TCaptureOutput ClassifyCapture(
    ui64 requestedOrdinal,
    bool preparationSucceeded,
    TVector<TRBOSemanticSnapshotBoundaryResultV1> boundaries,
    TStringBuf preparationIssues)
{
    if (requestedOrdinal == 0) {
        ythrow yexception() << "requested rule-application ordinal must be positive";
    }
    if (boundaries.size() != 2) {
        ythrow yexception()
            << "expected exactly two semantic snapshot boundaries, got "
            << boundaries.size();
    }

    auto& initial = boundaries[0];
    auto& candidate = boundaries[1];
    if (initial.Boundary != ERBOSemanticSnapshotBoundaryV1::Initial ||
        !initial.RuleApplications.empty())
    {
        ythrow yexception()
            << "first boundary must be Initial with no rule applications";
    }
    if (!initial.IsSupported()) {
        ythrow yexception()
            << "initial semantic snapshot is unsupported: "
            << initial.UnsupportedReason;
    }
    ValidateSnapshotResult(initial, "initial");

    const bool prefix = candidate.Boundary ==
        ERBOSemanticSnapshotBoundaryV1::RuleApplicationPrefix;
    const bool final = candidate.Boundary ==
        ERBOSemanticSnapshotBoundaryV1::Final;
    if (!prefix && !final) {
        ythrow yexception()
            << "second boundary must be RuleApplicationPrefix or Final";
    }
    if (prefix && preparationSucceeded) {
        ythrow yexception()
            << "preparation succeeded after an observed rule-prefix stop";
    }
    if (final && !preparationSucceeded) {
        ythrow yexception()
            << "optimizer preparation failed without an observed rule-prefix stop: "
            << preparationIssues;
    }

    const ui64 expectedApplications = prefix
        ? requestedOrdinal
        : candidate.RuleApplications.size();
    ValidateApplications(candidate.RuleApplications, expectedApplications);
    if (final && candidate.RuleApplications.size() >= requestedOrdinal) {
        ythrow yexception()
            << "Final boundary reached or exceeded the requested ordinal";
    }
    ValidateSnapshotResult(candidate, prefix ? "prefix" : "final");

    TCaptureOutput output;
    output.RequestedOrdinal = requestedOrdinal;
    output.InitialSnapshot = std::move(initial.Json);
    output.Applications = std::move(candidate.RuleApplications);
    if (candidate.IsSupported()) {
        output.Status = prefix
            ? ECaptureStatus::PrefixCaptured
            : ECaptureStatus::OptimizerComplete;
        output.CandidateSnapshot = std::move(candidate.Json);
    } else {
        output.Status = prefix
            ? ECaptureStatus::PrefixUnsupported
            : ECaptureStatus::FinalUnsupported;
        output.UnsupportedReason = std::move(candidate.UnsupportedReason);
    }
    ValidateOutput(output);
    return output;
}

TStringBuf StatusName(ECaptureStatus status) noexcept {
    switch (status) {
        case ECaptureStatus::PrefixCaptured:
            return "PREFIX_CAPTURED";
        case ECaptureStatus::PrefixUnsupported:
            return "PREFIX_UNSUPPORTED";
        case ECaptureStatus::OptimizerComplete:
            return "OPTIMIZER_COMPLETE";
        case ECaptureStatus::FinalUnsupported:
            return "FINAL_UNSUPPORTED";
    }
    return "UNKNOWN";
}

TString RenderManifest(const TCaptureOutput& capture) {
    ValidateOutput(capture);

    NJson::TJsonValue root(NJson::JSON_MAP);
    root["protocol"] = TString(Protocol);
    root["requested_ordinal"] = capture.RequestedOrdinal;
    root["status"] = TString(StatusName(capture.Status));
    root["initial_snapshot"] = TString(InitialSnapshotName);

    NJson::TJsonValue applications(NJson::JSON_ARRAY);
    for (const auto& application : capture.Applications) {
        NJson::TJsonValue item(NJson::JSON_MAP);
        item["ordinal"] = application.Ordinal;
        item["stage"] = application.StageName;
        item["rule"] = application.RuleName;
        applications.AppendValue(std::move(item));
    }
    root["applications"] = std::move(applications);

    switch (capture.Status) {
        case ECaptureStatus::PrefixCaptured:
            root["prefix_snapshot"] = TString(PrefixSnapshotName);
            break;
        case ECaptureStatus::OptimizerComplete:
            root["final_snapshot"] = TString(FinalSnapshotName);
            break;
        case ECaptureStatus::PrefixUnsupported:
        case ECaptureStatus::FinalUnsupported:
            root["unsupported_reason"] = capture.UnsupportedReason;
            break;
    }
    return NJson::WriteJson(root, true, true, true) + "\n";
}

TString RewriteBenchmarkSchemaToColumnStore(const TString& schema) {
    const std::regex table(
        R"(CREATE TABLE [^\(]+ \([^;]*\))",
        std::regex::multiline);
    return TString(std::regex_replace(
        std::string(schema.data(), schema.size()),
        table,
        "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);"));
}

TString AddBenchmarkQueryPrelude(const TString& query) {
    const TString prelude = R"(
$to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
$to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
$round = ($x,$y) -> { return $x; };
)";
    return prelude + query;
}

} // namespace NKikimr::NKqp::NRBOPrefixCapture

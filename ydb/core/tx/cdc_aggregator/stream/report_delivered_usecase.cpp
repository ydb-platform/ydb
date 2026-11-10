#include "report_delivered_usecase.h"

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

namespace {

bool ValidateDeliveredReport(const TDeliveredReport& report) {
    // A heartbeat is never at the minimum version, and the maximum version
    // would close the stream forever.
    if (report.Delivered.Version.IsMin() || report.Delivered.Version.IsMax()) {
        return false;
    }
    // The payload becomes a topic record once the version is the barrier.
    return !report.Delivered.Payload.empty() && report.Delivered.Payload.size() <= MaxBarrierPayloadBytes;
}

} // namespace

TReportDeliveredResult ReportDelivered(TStream& stream, IStreamRepository& repository, const TReportDeliveredCommand& command) {
    using EStatus = TReportDeliveredResult::EStatus;
    using ESourceStatus = TReportDeliveredResult::TSourceResult::EStatus;

    TReportDeliveredResult result;
    if (!stream.IsConfigured()) {
        result.Status = EStatus::NotReady;
        result.Error = "Stream is not configured";
        return result;
    }
    if (command.StreamPathId != stream.GetConfig().StreamPathId) {
        result.Status = EStatus::WrongStream;
        result.Error = TStringBuilder() << "Aggregator serves stream " << stream.GetConfig().StreamPathId;
        return result;
    }
    if (command.Reports.size() > MaxReportsPerBatch) {
        result.Status = EStatus::BadRequest;
        result.Error = TStringBuilder() << "Batch has " << command.Reports.size() << " reports, the limit is " << MaxReportsPerBatch;
        return result;
    }

    TStreamChanges changes;
    result.Results.reserve(command.Reports.size());
    for (const auto& report : command.Reports) {
        auto& sourceResult = result.Results.emplace_back();
        sourceResult.Source = report.Source;

        if (!ValidateDeliveredReport(report)) {
            sourceResult.Status = ESourceStatus::BadRequest;
            continue;
        }

        switch (stream.ApplyDelivered(report.Source, report.Delivered, changes)) {
            case EDeliveredOutcome::Advanced:
                sourceResult.Advanced = true;
                break;
            case EDeliveredOutcome::NotAdvanced:
                break;
            case EDeliveredOutcome::UnknownSource:
                sourceResult.Status = ESourceStatus::UnknownSource;
                break;
            case EDeliveredOutcome::RetiredSource:
                sourceResult.Status = ESourceStatus::RetiredSource;
                break;
        }
    }

    // The batch commits as a whole, so every accepted report of a source is
    // confirmed with the final progress of that source.
    for (auto& sourceResult : result.Results) {
        if (sourceResult.Status == ESourceStatus::Success) {
            const auto* progress = stream.FindSourceProgress(sourceResult.Source);
            Y_ENSURE(progress && *progress, "Accepted source " << sourceResult.Source << " has no progress");
            sourceResult.PersistedVersion = (*progress)->Version;
        }
    }

    if (changes.ReadyBarrier) {
        result.AdvancedReadyBarrier = stream.GetReadyBarrier();
    }
    if (!changes.IsEmpty()) {
        repository.Save(stream, changes);
    }

    return result;
}

} // namespace NKikimr::NCdcAggregator

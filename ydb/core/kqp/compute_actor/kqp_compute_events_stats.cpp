#include "kqp_compute_events_stats.h"
#include <util/string/builder.h>

namespace NKikimr::NKqp {


TString FormatDurationAsMilliseconds(TDuration duration) {
    return TStringBuilder() << duration.MicroSeconds() / 1000.0 << "ms";
}

TString TPerStepScanStatistics::DebugString() const {
    return TStringBuilder() << "StepName: " << StepName << ";"
                            << "IntegralExecutionDuration:" << FormatDurationAsMilliseconds(IntegralExecutionDuration) << ";"
                            << "IntegralWaitDuration:" << FormatDurationAsMilliseconds(IntegralWaitDuration) << ";"
                            << "DeltaRawBytesRead:" << DeltaRawBytesRead << ";";
}

void TColumnShardScanStatsCollection::Merge(const NKqpProto::TColumnShardScanStats& report) {
    if (report.GetStatsScanId().empty()) {
        return;
    }
    auto& scan = Scans[report.GetStatsScanId()];
    scan.Context.SetStatsScanId(report.GetStatsScanId());
    if (report.HasTabletId()) {
        scan.Context.SetTabletId(report.GetTabletId());
    }
    if (report.HasReaderKind()) {
        scan.Context.SetReaderKind(report.GetReaderKind());
    }
    if (report.HasStatsGroupId()) {
        scan.Context.SetStatsGroupId(report.GetStatsGroupId());
    }
    if (report.HasParticipatingShards()) {
        scan.Context.SetParticipatingShards(report.GetParticipatingShards());
    }
    if (report.HasExpectedContextOnly()) {
        scan.Context.SetExpectedContextOnly(report.GetExpectedContextOnly());
    }
    if (report.ParticipatingTabletsSize()) {
        scan.Context.ClearParticipatingTablets();
        for (ui64 tabletId : report.GetParticipatingTablets()) {
            scan.Context.AddParticipatingTablets(tabletId);
        }
    }
    scan.Context.SetFinished(scan.Context.GetFinished() || report.GetFinished());
    if (scan.Context.GetFinished()) {
        scan.Context.SetPendingOperations(0);
    } else if (report.HasPendingOperations()) {
        scan.Context.SetPendingOperations(report.GetPendingOperations());
    }
    scan.Context.SetTruncated(scan.Context.GetTruncated() || report.GetTruncated());
    if (report.HasCacheStats()) {
        auto* cache = scan.Context.MutableCacheStats();
        const auto& update = report.GetCacheStats();
        if (update.HasCompletedReadRanges()) {
            cache->SetCompletedReadRanges(Max(cache->GetCompletedReadRanges(), update.GetCompletedReadRanges()));
        }
        if (update.HasCacheServedReadRanges()) {
            cache->SetCacheServedReadRanges(Max(cache->GetCacheServedReadRanges(), update.GetCacheServedReadRanges()));
        }
        if (update.HasReturnedBytes()) {
            cache->SetReturnedBytes(Max(cache->GetReturnedBytes(), update.GetReturnedBytes()));
        }
        if (update.HasCacheServedBytes()) {
            cache->SetCacheServedBytes(Max(cache->GetCacheServedBytes(), update.GetCacheServedBytes()));
        }
    }
    for (const auto& step : report.GetSteps()) {
        scan.Steps[step.GetStepId()].CopyFrom(step);
    }
}

void TColumnShardScanStatsCollection::Export(google::protobuf::RepeatedPtrField<NKqpProto::TColumnShardScanStats>* dst) const {
    dst->Clear();
    for (const auto& [_, scan] : Scans) {
        auto* report = dst->Add();
        report->CopyFrom(scan.Context);
        for (const auto& [stepId, step] : scan.Steps) {
            report->AddSteps()->CopyFrom(step);
        }
    }
}

}

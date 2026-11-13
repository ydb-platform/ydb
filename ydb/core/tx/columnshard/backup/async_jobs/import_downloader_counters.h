#pragma once

#include <ydb/core/tx/columnshard/counters/inflight_guard.h>

#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <optional>

namespace NKikimr::NColumnShard::NBackup {

class TImportDownloaderCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr AliveActors;
    NMonitoring::TDynamicCounters::TCounterPtr ProcessInflight;

    NMonitoring::TDynamicCounters::TCounterPtr ErrorsCount;
    NMonitoring::TDynamicCounters::TCounterPtr RowsProcessed;
    NMonitoring::TDynamicCounters::TCounterPtr BatchesProcessed;
    NMonitoring::TDynamicCounters::TCounterPtr BytesProcessed;

    NMonitoring::THistogramPtr HistogramProcessDurationMs;

    std::optional<NOlap::NCounters::TInflightGuard> AliveGuard;
    std::optional<NOlap::NCounters::TInflightGuard> ProcessGuard;

public:
    TImportDownloaderCounters()
        : TBase("ImportDownloader")
    {
        AliveActors = TBase::GetValue("ImportDownloader/Actors/Alive");
        ProcessInflight = TBase::GetValue("ImportDownloader/Process/Inflight");

        ErrorsCount = TBase::GetDeriviative("ImportDownloader/Errors/Count");
        RowsProcessed = TBase::GetDeriviative("ImportDownloader/Rows/Processed");
        BatchesProcessed = TBase::GetDeriviative("ImportDownloader/Batches/Processed");
        BytesProcessed = TBase::GetDeriviative("ImportDownloader/Bytes/Processed");

        HistogramProcessDurationMs =
            TBase::GetHistogram("ImportDownloader/Process/DurationMs/Histogram", NMonitoring::ExponentialHistogram(20, 2, 1));
    }

    void OnActorAlive() {
        AliveGuard.emplace(AliveActors);
    }

    void OnProcessStarted() {
        ProcessGuard.emplace(ProcessInflight);
    }

    void OnProcessFinished(const TDuration duration, ui64 rows, ui64 bytes) {
        ProcessGuard.reset();
        HistogramProcessDurationMs->Collect(duration.MilliSeconds());
        *RowsProcessed += rows;
        *BytesProcessed += bytes;
        BatchesProcessed->Inc();
    }

    void OnError() const {
        ErrorsCount->Inc();
    }
};

}   // namespace NKikimr::NColumnShard::NBackup

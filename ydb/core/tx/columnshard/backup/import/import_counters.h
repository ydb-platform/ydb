
#pragma once

#include <ydb/core/tx/columnshard/counters/inflight_guard.h>

#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <optional>

namespace NKikimr::NOlap::NImport {

class TImportCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr ReadInflight;
    NMonitoring::TDynamicCounters::TCounterPtr WriteInflight;
    NMonitoring::TDynamicCounters::TCounterPtr SaveProgressInflight;
    NMonitoring::TDynamicCounters::TCounterPtr AliveActors;

    NMonitoring::TDynamicCounters::TCounterPtr ErrorsCount;
    NMonitoring::TDynamicCounters::TCounterPtr BatchesWritten;
    NMonitoring::TDynamicCounters::TCounterPtr BatchesReceived;

    NMonitoring::THistogramPtr HistogramReadDurationMs;
    NMonitoring::THistogramPtr HistogramWriteDurationMs;
    NMonitoring::THistogramPtr HistogramSaveProgressDurationMs;

    std::optional<NCounters::TInflightGuard> AliveGuard;
    std::optional<NCounters::TInflightGuard> ReadGuard;
    std::optional<NCounters::TInflightGuard> WriteGuard;
    std::optional<NCounters::TInflightGuard> SaveProgressGuard;

public:
    TImportCounters()
        : TBase("ImportActor")
    {
        ReadInflight = TBase::GetValue("Import/Read/Inflight");
        WriteInflight = TBase::GetValue("Import/Write/Inflight");
        SaveProgressInflight = TBase::GetValue("Import/SaveProgress/Inflight");
        AliveActors = TBase::GetValue("Import/Actors/Alive");

        ErrorsCount = TBase::GetDeriviative("Import/Errors/Count");
        BatchesWritten = TBase::GetDeriviative("Import/Batches/Written");
        BatchesReceived = TBase::GetDeriviative("Import/Batches/Received");

        HistogramReadDurationMs = TBase::GetHistogram("Import/Read/DurationMs/Histogram", NMonitoring::ExponentialHistogram(20, 2, 1));
        HistogramWriteDurationMs = TBase::GetHistogram("Import/Write/DurationMs/Histogram", NMonitoring::ExponentialHistogram(20, 2, 1));
        HistogramSaveProgressDurationMs =
            TBase::GetHistogram("Import/SaveProgress/DurationMs/Histogram", NMonitoring::ExponentialHistogram(20, 2, 1));
    }

    void OnReadStarted() {
        ReadGuard.emplace(ReadInflight);
    }

    void OnReadFinished(const TDuration duration) {
        ReadGuard.reset();
        HistogramReadDurationMs->Collect(duration.MilliSeconds());
        BatchesReceived->Inc();
    }

    void OnWriteStarted() {
        WriteGuard.emplace(WriteInflight);
    }

    void OnWriteFinished(const TDuration duration) {
        WriteGuard.reset();
        HistogramWriteDurationMs->Collect(duration.MilliSeconds());
        BatchesWritten->Inc();
    }

    void OnSaveProgressStarted() {
        SaveProgressGuard.emplace(SaveProgressInflight);
    }

    void OnSaveProgressFinished(const TDuration duration) {
        SaveProgressGuard.reset();
        HistogramSaveProgressDurationMs->Collect(duration.MilliSeconds());
    }

    void OnOperationsCancelled() {
        ReadGuard.reset();
        WriteGuard.reset();
        SaveProgressGuard.reset();
    }

    void OnError() const {
        ErrorsCount->Inc();
    }

    void OnActorAlive() {
        AliveGuard.emplace(AliveActors);
    }
};

}   // namespace NKikimr::NOlap::NImport


#pragma once

#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

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

    void OnReadStarted() const {
        ReadInflight->Inc();
    }

    void OnReadFinished(const TDuration duration) const {
        ReadInflight->Dec();
        HistogramReadDurationMs->Collect(duration.MilliSeconds());
        BatchesReceived->Inc();
    }

    void OnWriteStarted() const {
        WriteInflight->Inc();
    }

    void OnWriteFinished(const TDuration duration) const {
        WriteInflight->Dec();
        HistogramWriteDurationMs->Collect(duration.MilliSeconds());
        BatchesWritten->Inc();
    }

    void OnSaveProgressStarted() const {
        SaveProgressInflight->Inc();
    }

    void OnSaveProgressFinished(const TDuration duration) const {
        SaveProgressInflight->Dec();
        HistogramSaveProgressDurationMs->Collect(duration.MilliSeconds());
    }

    void OnError() const {
        ErrorsCount->Inc();
    }

    void OnActorAlive() const {
        AliveActors->Inc();
    }

    void OnActorDead() const {
        AliveActors->Dec();
    }
};

}   // namespace NKikimr::NOlap::NImport

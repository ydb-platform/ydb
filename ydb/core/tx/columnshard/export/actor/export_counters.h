
#pragma once

#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NOlap::NExport {

class TExportCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr ReadInflight;
    NMonitoring::TDynamicCounters::TCounterPtr WriteInflight;
    NMonitoring::TDynamicCounters::TCounterPtr CursorInflight;
    NMonitoring::TDynamicCounters::TCounterPtr AckInflight;
    NMonitoring::TDynamicCounters::TCounterPtr AliveActors;

    NMonitoring::TDynamicCounters::TCounterPtr ErrorsCount;
    NMonitoring::TDynamicCounters::TCounterPtr BatchesWritten;
    NMonitoring::TDynamicCounters::TCounterPtr AckSent;

    NMonitoring::THistogramPtr HistogramReadDurationMs;
    NMonitoring::THistogramPtr HistogramWriteDurationMs;
    NMonitoring::THistogramPtr HistogramSaveCursorDurationMs;

public:
    TExportCounters()
        : TBase("ExportActor")
    {
        ReadInflight = TBase::GetValue("Export/Read/Inflight");
        WriteInflight = TBase::GetValue("Export/Write/Inflight");
        CursorInflight = TBase::GetValue("Export/SaveCursor/Inflight");
        AckInflight = TBase::GetValue("Export/Ack/Inflight");
        AliveActors = TBase::GetValue("Export/Actors/Alive");

        ErrorsCount = TBase::GetDeriviative("Export/Errors/Count");
        BatchesWritten = TBase::GetDeriviative("Export/Batches/Written");
        AckSent = TBase::GetDeriviative("Export/Ack/Sent");

        HistogramReadDurationMs = TBase::GetHistogram("Export/Read/DurationMs/Histogram", NMonitoring::ExponentialHistogram(20, 2, 1));
        HistogramWriteDurationMs = TBase::GetHistogram("Export/Write/DurationMs/Histogram", NMonitoring::ExponentialHistogram(20, 2, 1));
        HistogramSaveCursorDurationMs =
            TBase::GetHistogram("Export/SaveCursor/DurationMs/Histogram", NMonitoring::ExponentialHistogram(20, 2, 1));
    }

    void OnAckSent() const {
        AckSent->Inc();
        AckInflight->Inc();
    }

    void OnAckResponse() const {
        AckInflight->Dec();
    }

    void OnReadStarted() const {
        ReadInflight->Inc();
    }

    void OnReadFinished(const TDuration duration) const {
        ReadInflight->Dec();
        HistogramReadDurationMs->Collect(duration.MilliSeconds());
    }

    void OnWriteStarted() const {
        WriteInflight->Inc();
    }

    void OnWriteFinished(const TDuration duration) const {
        WriteInflight->Dec();
        HistogramWriteDurationMs->Collect(duration.MilliSeconds());
        BatchesWritten->Inc();
    }

    void OnSaveCursorStarted() const {
        CursorInflight->Inc();
    }

    void OnSaveCursorFinished(const TDuration duration) const {
        CursorInflight->Dec();
        HistogramSaveCursorDurationMs->Collect(duration.MilliSeconds());
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

}   // namespace NKikimr::NOlap::NExport

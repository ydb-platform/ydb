
#pragma once

#include <ydb/core/tx/columnshard/counters/inflight_guard.h>

#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <optional>

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

    std::optional<NCounters::TInflightGuard> AliveGuard;
    std::optional<NCounters::TInflightGuard> ReadGuard;
    std::optional<NCounters::TInflightGuard> WriteGuard;
    std::optional<NCounters::TInflightGuard> CursorGuard;
    std::optional<NCounters::TInflightGuard> AckGuard;

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

    void OnAckSent() {
        AckSent->Inc();
        AckGuard.emplace(AckInflight);
    }

    void OnAckResponse() {
        AckGuard.reset();
    }

    void OnReadStarted() {
        ReadGuard.emplace(ReadInflight);
    }

    void OnReadFinished(const TDuration duration) {
        ReadGuard.reset();
        HistogramReadDurationMs->Collect(duration.MilliSeconds());
    }

    void OnWriteStarted() {
        WriteGuard.emplace(WriteInflight);
    }

    void OnWriteFinished(const TDuration duration) {
        WriteGuard.reset();
        HistogramWriteDurationMs->Collect(duration.MilliSeconds());
        BatchesWritten->Inc();
    }

    void OnSaveCursorStarted() {
        CursorGuard.emplace(CursorInflight);
    }

    void OnSaveCursorFinished(const TDuration duration) {
        CursorGuard.reset();
        HistogramSaveCursorDurationMs->Collect(duration.MilliSeconds());
    }

    void OnOperationsCancelled() {
        ReadGuard.reset();
        WriteGuard.reset();
        CursorGuard.reset();
        AckGuard.reset();
    }

    void OnError() const {
        ErrorsCount->Inc();
    }

    void OnActorAlive() {
        AliveGuard.emplace(AliveActors);
    }
};

}   // namespace NKikimr::NOlap::NExport

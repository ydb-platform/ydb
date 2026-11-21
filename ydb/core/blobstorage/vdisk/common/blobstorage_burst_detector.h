#pragma once

#include <ydb/core/util/light.h>

#include <array>

namespace NKikimr {

// Per-VDisk disk-budget exhaustion metrics with bounded reordering. A missing sequence
// number is tolerated for at most 255 subsequent positions or one second (checked
// on Set/Update). Beyond that, discard pending history and adopt the newest sample.
// Count and durations are approximate across such a resynchronization.
class TBurstDetector : public TLightBase {
    struct TItem {
        bool State = false;
        bool Filled = false;
    };

    static constexpr size_t Capacity = 256;
    TSpinLock Lock;
    std::array<TItem, Capacity> Data{};
    size_t HeadIdx = 0;
    size_t FilledCount = 0;
    ui64 Seqno = 0;
    ui64 NewestSeqno = 0;
    bool NewestState = false;
    NHPTimer::STime PendingSince = 0;
    NMonitoring::TDynamicCounters::TCounterPtr ResyncCount;

public:
    void Initialize(TIntrusivePtr<NMonitoring::TDynamicCounters>& counters) {
        TLightBase::Initialize(counters, TLightCounterConfig::WithDefaultLightSet("BurstDetector"));
        ResyncCount = counters->GetCounter("BurstDetector_resyncs", true);
    }

    void Set(bool state, ui64 seqno) {
        TGuard<TSpinLock> g(Lock);
        const NHPTimer::STime now = Now();
        RecoverExpiredGap(now);
        if (!FilledCount) {
            Reset(state, seqno, now, false);
            return;
        }

        const ui64 diff = seqno - Seqno;
        if (!diff) {
            return; // Duplicate observation must not crash the process.
        }
        if (diff > Max<ui64>() / 2) {
            return; // Late observations must not undo recovery.
        }
        if (diff >= Capacity) {
            Reset(state, seqno, now, true);
            return;
        }

        TItem& item = Data[(HeadIdx + diff) % Capacity];
        if (item.Filled) {
            return; // Duplicate pending observation.
        }
        item = {state, true};
        if (FilledCount++ == 1) {
            PendingSince = now;
        }
        if (diff > NewestSeqno - Seqno) {
            NewestSeqno = seqno;
            NewestState = state;
        }

        while (Data[(HeadIdx + 1) % Capacity].Filled) {
            const bool prevState = Data[HeadIdx].State;
            Data[HeadIdx].Filled = false;
            HeadIdx = (HeadIdx + 1) % Capacity;
            ++Seqno;
            --FilledCount;
            Advance(prevState, now);
            Modify(Data[HeadIdx].State, prevState);
        }
        if (FilledCount == 1) {
            PendingSince = 0;
        }
    }

    void Update() {
        TGuard<TSpinLock> g(Lock);
        const NHPTimer::STime now = Now();
        RecoverExpiredGap(now);
        Advance(Data[HeadIdx].State, now);
    }

private:
    void RecoverExpiredGap(NHPTimer::STime now) {
        if (FilledCount > 1 && ui64(now - PendingSince) >= HPCyclesMs(1000)) {
            Reset(NewestState, NewestSeqno, now, true);
        }
    }

    void Reset(bool state, ui64 seqno, NHPTimer::STime now, bool resync) {
        const bool prevState = Data[HeadIdx].State;
        // Charge the unknown interval to the previously observed state. Never
        // rewrite published time backwards or invent durations for lost edges.
        Advance(prevState, now);
        Modify(state, prevState);
        Data.fill({});
        HeadIdx = 0;
        Data[0] = {state, true};
        FilledCount = 1;
        Seqno = NewestSeqno = seqno;
        NewestState = state;
        PendingSince = 0;
        if (resync) {
            ++*ResyncCount;
        }
    }
};

} // namespace NKikimr

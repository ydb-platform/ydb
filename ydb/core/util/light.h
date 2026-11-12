#pragma once

#include <array>
#include <optional>
#include <type_traits>

#include <ydb/core/mon/mon.h>

#include "hp_timer_helpers.h"

namespace NKikimr {

class TLightCounterConfig {
public:
    std::optional<TString> StateName;
    std::optional<TString> CountName;
    std::optional<TString> RedMsName;
    std::optional<TString> GreenMsName;
    std::optional<TString> ResyncCountName;

    static TLightCounterConfig Create() {
        return {};
    }

    TLightCounterConfig& WithState(const TString& name) {
        StateName = name;
        return *this;
    }

    TLightCounterConfig& WithCount(const TString& name) {
        CountName = name;
        return *this;
    }

    TLightCounterConfig& WithRedMs(const TString& name) {
        RedMsName = name;
        return *this;
    }

    TLightCounterConfig& WithGreenMs(const TString& name) {
        GreenMsName = name;
        return *this;
    }

    TLightCounterConfig& WithResyncCount(const TString& name) {
        ResyncCountName = name;
        return *this;
    }

    static TLightCounterConfig WithDefaultLightSet(const TString& baseName) {
        TLightCounterConfig config;
        config.StateName = baseName + "_state";
        config.CountName = baseName + "_count";
        config.RedMsName = baseName + "_redMs";
        config.GreenMsName = baseName + "_greenMs";
        return config;
    }
};

class TLightBase {
protected:
    TString Name;
    ::NMonitoring::TDynamicCounters::TCounterPtr State; // Current state (0=OFF=green, 1=ON=red)
    ::NMonitoring::TDynamicCounters::TCounterPtr Count; // Number of switches to ON state
    ::NMonitoring::TDynamicCounters::TCounterPtr RedMs; // Time elapsed in ON state
    ::NMonitoring::TDynamicCounters::TCounterPtr GreenMs; // Time elapsed in OFF state
    ::NMonitoring::TDynamicCounters::TCounterPtr ResyncCount;
private:
    ui64 RedCycles = 0;
    ui64 GreenCycles = 0;
    NHPTimer::STime AdvancedTill = 0;
    NHPTimer::STime LastNow = 0;
    ui64 UpdateThreshold = 0;
public:
    void Initialize(TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, const TLightCounterConfig& config) {
        if (config.StateName) {
            State = counters->GetCounter(*config.StateName);
        }
        if (config.CountName) {
            Count = counters->GetCounter(*config.CountName, true);
        }
        if (config.RedMsName) {
            RedMs = counters->GetCounter(*config.RedMsName, true);
        }
        if (config.GreenMsName) {
            GreenMs = counters->GetCounter(*config.GreenMsName, true);
        }
        if (config.ResyncCountName) {
            ResyncCount = counters->GetCounter(*config.ResyncCountName, true);
        }
        UpdateThreshold = HPCyclesMs(100);
        AdvancedTill = Now();
    }

    ui64 GetCount() const {
        return *Count;
    }

    ui64 GetRedMs() const {
        return *RedMs;
    }

    ui64 GetGreenMs() const {
        return *GreenMs;
    }
protected:
    void Modify(bool state, bool prevState) {
        if (state && !prevState) { // Switched to ON state
            if (State) {
                *State = true;
            }
            if (Count) {
                (*Count)++;
            }
            return;
        }
        if (!state && prevState) { // Switched to OFF state
            if (State) {
                *State = false;
            }
            return;
        }
    }

    void Advance(bool state, NHPTimer::STime now) {
        if (now == AdvancedTill) {
            return;
        }
        Elapsed(state, now - AdvancedTill);
        if (RedMs && RedCycles > UpdateThreshold) {
            *RedMs += CutMs(RedCycles);
        }
        if (GreenMs && GreenCycles > UpdateThreshold) {
            *GreenMs += CutMs(GreenCycles);
        }
        AdvancedTill = now;
    }

    NHPTimer::STime Now() {
        // Avoid time going backwards
        NHPTimer::STime now = HPNow();
        if (now < LastNow) {
            now = LastNow;
        }
        LastNow = now;
        return now;
    }
private:
    void Elapsed(bool state, ui64 cycles) {
        if (state) {
            RedCycles += cycles;
        } else {
            GreenCycles += cycles;
        }
    }

    ui64 CutMs(ui64& src) {
        ui64 ms = HPMilliSeconds(src);
        ui64 cycles = HPCyclesMs(ms);
        src -= cycles;
        return ms;
    }
};

// Diagnostic state/time accounting with bounded reordering. A missing sequence
// number is tolerated for at most 255 subsequent positions or one second (checked
// on Set/Update). Beyond that, discard pending history and adopt the newest sample.
// Count and durations are approximate across such a resynchronization.
// Prefer ui64 producers: a ui16 producer cannot distinguish a long delay from wrap.
template <typename TSeqno>
class TBasicLight : public TLightBase {
    static_assert(std::is_same_v<TSeqno, ui16> || std::is_same_v<TSeqno, ui64>);

    struct TItem {
        bool State = false;
        bool Filled = false;
    };

    static constexpr size_t Capacity = 256;
    TSpinLock Lock;
    std::array<TItem, Capacity> Data{};
    size_t HeadIdx = 0;
    size_t FilledCount = 0;
    TSeqno Seqno = 0;
    TSeqno NewestSeqno = 0;
    bool NewestState = false;
    NHPTimer::STime PendingSince = 0;

public:
    void Set(bool state, TSeqno seqno) {
        TGuard<TSpinLock> g(Lock);
        const auto now = Now();
        RecoverExpiredGap(now);
        if (!FilledCount) {
            Reset(state, seqno, now, false);
            return;
        }

        const TSeqno diff = seqno - Seqno;
        if (!diff) {
            return; // Duplicate observation must not crash the process.
        }
        if (diff > Max<TSeqno>() / 2) {
            // Nearby late samples, and old samples with a full-width sequence,
            // must not roll the state back after initialization or recovery.
            const TSeqno backwards = Seqno - seqno;
            if constexpr (std::is_same_v<TSeqno, ui64>) {
                return;
            } else if (backwards < Capacity) {
                return;
            }
            // For ui16 a large backwards distance is ambiguous. Recover rather
            // than wait for half a sequence cycle with a potentially stale state.
            Reset(state, seqno, now, true);
            return;
        }
        if (diff >= Capacity) {
            Reset(state, seqno, now, true);
            return;
        }

        auto& item = Data[(HeadIdx + diff) % Capacity];
        if (item.Filled) {
            return; // Duplicate pending observation.
        }
        item = {state, true};
        if (FilledCount++ == 1) {
            PendingSince = now;
        }
        if (diff > TSeqno(NewestSeqno - Seqno)) {
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
        const auto now = Now();
        RecoverExpiredGap(now);
        Advance(Data[HeadIdx].State, now);
    }

private:
    void RecoverExpiredGap(NHPTimer::STime now) {
        if (FilledCount > 1 && ui64(now - PendingSince) >= HPCyclesMs(1000)) {
            Reset(NewestState, NewestSeqno, now, true);
        }
    }

    void Reset(bool state, TSeqno seqno, NHPTimer::STime now, bool resync) {
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
        if (resync && ResyncCount) {
            ++*ResyncCount;
        }
    }
};

using TLight = TBasicLight<ui16>;
using TLight64 = TBasicLight<ui64>;

} // namespace NKikimr

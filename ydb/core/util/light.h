#pragma once

#include <optional>
#include <type_traits>

#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <ydb/core/mon/mon.h>

#include "hp_timer_helpers.h"

namespace NKikimr {

class TLightCounterConfig {
public:
    std::optional<TString> StateName;
    std::optional<TString> CountName;
    std::optional<TString> RedMsName;
    std::optional<TString> GreenMsName;

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

// Thread-safe light
// State changes are serialized by an internal lock. The callable may also mutate
// state whose access is confined to these callbacks, making the mutation and its
// monitoring transition one ordered operation. If shared state is changed outside
// the callback, Set only samples it: rapid intermediate flips may be coalesced and
// do not affect the switch count. Computing a state before the call is unsafe
// because it may be stale by the time the lock is acquired.
class TLight : public TLightBase {
private:
    TSpinLock Lock;
    bool CurrentState = false;
public:
    // computeState is invoked under a spin lock, it must be cheap and non-blocking
    template <typename TComputeState, typename = std::enable_if_t<std::is_invocable_r_v<bool, TComputeState&>>>
    void Set(TComputeState&& computeState) {
        TGuard<TSpinLock> g(Lock);
        const bool state = computeState();
        Advance(CurrentState, Now());
        Modify(state, CurrentState);
        CurrentState = state;
    }

    // Only for states not derived from shared data (single writer thread or
    // independent per-event values); otherwise use the callable overload
    void Set(bool state) {
        Set([state] { return state; });
    }

    void Update() {
        TGuard<TSpinLock> g(Lock);
        Advance(CurrentState, Now());
    }
};

} // namespace NKikimr

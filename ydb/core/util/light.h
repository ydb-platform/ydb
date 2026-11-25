#pragma once

#include <optional>

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

// Thread-safe light for ui16 sequence numbers within a half-range ordering window.
// The first observation establishes the baseline; late older observations are ignored.
class TLight : public TLightBase {
private:
    struct TItem {
        bool State;
        bool Filled;
        TItem(bool state = false, bool filled = false)
            : State(state)
            , Filled(filled)
        {}
    };

    // Cyclic buffer to enforce event ordering by seqno
    TSpinLock Lock;
    size_t HeadIdx = 0; // Index of current state
    size_t FilledCount = 0;
    ui16 Seqno = 0; // Current seqno
    TStackVec<TItem, 32> Data;
public:
    TLight() {
        InitData();
    }

    void Set(bool state, ui16 seqno) {
        TGuard<TSpinLock> g(Lock);
        Push(state, seqno);
        bool prevState;
        // Note that 'state' variable is being reused
        NHPTimer::STime now = Now();
        while (Pop(state, prevState)) {
            Modify(state, prevState);
            Advance(prevState, now);
        }
    }

    void Update() {
        TGuard<TSpinLock> g(Lock);
        Advance(Data[HeadIdx].State, Now());
    }

private:
    void InitData(bool state = false, bool filled = false) {
        Data.clear();
        Data.emplace_back(state, filled);
        Data.resize(32);
        HeadIdx = 0;
    }

    void Push(bool state, ui16 seqno) {
        if (!FilledCount) { // First event must initialize seqno
            FilledCount = 1;
            Seqno = seqno;
            InitData(state, true);
            if (state) {
                Modify(true, false);
            }
            return;
        }
        const ui16 diff = seqno - Seqno;
        if (!diff || diff > Max<ui16>() / 2) {
            // A producer can obtain its seqno first but enter Set after another
            // producer initialized the light. Do not treat an old sample as a
            // future observation. Ignore duplicates without altering FilledCount.
            return;
        }
        size_t size = Data.size();
        if (size <= diff) { // Buffer is full -- extend and move wrapped part
            size_t newSize = size * 2;
            while (newSize <= diff) {
                newSize *= 2;
            }
            Data.resize(newSize);
            for (size_t i = 0; i < HeadIdx; i++) {
                Data[size + i] = Data[i];
                Data[i].Filled = false;
            }
        }
        TItem& item = Data[(HeadIdx + diff) % Data.size()];
        if (item.Filled) {
            return; // Duplicate pending observation.
        }
        item.Filled = true;
        item.State = state;
        ++FilledCount;
    }

    bool Pop(bool& state, bool& prevState) {
        size_t nextIdx = (HeadIdx + 1) % Data.size();
        TItem& head = Data[HeadIdx];
        TItem& next = Data[nextIdx];
        if (!head.Filled || !next.Filled) {
            return false;
        }
        state = next.State;
        prevState = head.State;
        head.Filled = false;
        HeadIdx = nextIdx;
        Seqno++; // Overflow is fine
        FilledCount--;
        if (FilledCount == 1 && Data.size() > 32) {
            InitData(state, true);
        }
        return true;
    }
};

} // namespace NKikimr

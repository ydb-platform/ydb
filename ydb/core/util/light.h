#include <ydb/core/mon/mon.h>

#include "hp_timer_helpers.h"

namespace NKikimr {

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
    void Initialize(TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, const TString& name) {
        Name = name;
        State = counters->GetCounter(name + "_state");
        Count = counters->GetCounter(name + "_count", true);
        RedMs = counters->GetCounter(name + "_redMs", true);
        GreenMs = counters->GetCounter(name + "_greenMs", true);
        UpdateThreshold = HPCyclesMs(100);
        AdvancedTill = Now();
    }

    void Initialize(TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, const TString& countName,
            const TString& redMsName,const TString& greenMsName) {
        Count = counters->GetCounter(countName, true);
        RedMs = counters->GetCounter(redMsName, true);
        GreenMs = counters->GetCounter(greenMsName, true);
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
            (*Count)++;
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
        if (RedCycles > UpdateThreshold) {
            *RedMs += CutMs(RedCycles);
        }
        if (GreenCycles > UpdateThreshold) {
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
    TStackVec<TItem, 32> Data; // In theory should have not more than thread count items
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
        FilledCount++;
        if (FilledCount == 1) { // First event must initialize seqno
            Seqno = seqno;
            InitData(state, true);
            if (state) {
                Modify(true, false);
            }
            return;
        }
        Y_ABORT_UNLESS(seqno != Seqno, "ordering overflow or duplicate event headSeqno# %d seqno# %d state# %d filled# %d",
                 (int)Seqno, (int)seqno, (int)state, (int)CountFilled());
        ui16 diff = seqno;
        diff -= Seqno; // Underflow is fine
        size_t size = Data.size();
        if (size <= diff) { // Buffer is full -- extend and move wrapped part
            Data.resize(size * 2);
            for (size_t i = 0; i < HeadIdx; i++) {
                Data[size + i] = Data[i];
                Data[i].Filled = false;
            }
        }
        TItem& item = Data[(HeadIdx + diff) % Data.size()];
        Y_ABORT_UNLESS(!item.Filled, "ordering overflow or duplicate event headSeqno# %d seqno# %d state# %d filled# %d",
                 (int)Seqno, (int)seqno, (int)state, (int)CountFilled());
        item.Filled = true;
        item.State = state;
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

    size_t CountFilled() const {
        size_t ret = 0;
        for (const TItem& item : Data) {
            ret += item.Filled;
        }
        return ret;
    }
};

} // namespace NKikimr

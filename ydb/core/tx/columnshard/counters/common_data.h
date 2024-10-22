#pragma once
#include "common/owner.h"

namespace NKikimr::NColumnShard {

class TDataOwnerSignals: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    const TString DataName;
    NMonitoring::TDynamicCounters::TCounterPtr AddCount;
    NMonitoring::TDynamicCounters::TCounterPtr AddBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SkipAddCount;
    NMonitoring::TDynamicCounters::TCounterPtr SkipAddBytes;

    NMonitoring::TDynamicCounters::TCounterPtr EraseCount;
    NMonitoring::TDynamicCounters::TCounterPtr EraseBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SkipEraseCount;
    NMonitoring::TDynamicCounters::TCounterPtr SkipEraseBytes;
    std::shared_ptr<TValueAggregationClient> DataSize;
    std::shared_ptr<TValueAggregationClient> ChunksCount;
public:
    TDataOwnerSignals(const TString& module, const TString dataName);

    i64 GetDataSize() const {
        return DataSize->GetValueSimple();
    }

    void Add(const ui64 size, const bool load) const {
        DataSize->Add(size);
        ChunksCount->Add(1);
        if (!load) {
            AddCount->Add(1);
            AddBytes->Add(size);
        }
    }

    void Erase(const ui64 size) const {
        DataSize->Remove(size);
        ChunksCount->Remove(1);
        EraseCount->Add(1);
        EraseBytes->Add(size);
    }

    void SkipAdd(const ui64 size) const {
        SkipAddCount->Add(1);
        SkipAddBytes->Add(size);
    }

    void SkipErase(const ui64 size) const {
        SkipEraseCount->Add(1);
        SkipEraseBytes->Add(size);
    }

};

class TLoadTimeSignals: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr LoadingTimeCounter;
    NMonitoring::TDynamicCounters::TCounterPtr LoadingFailCounter;

public:
    TLoadTimeSignals(const TString& type)
        : TBase("Startup")
    {
        LoadingTimeCounter = TBase::GetValue("Startup/" + type + "LoadingTime");;
        LoadingFailCounter = TBase::GetValue("Startup/" + type + "LoadingFailCount");;
    }

    void AddLoadingTime(ui64 microSeconds) {
        LoadingTimeCounter->Add(microSeconds);
    }

    void AddLoadingFail() {
        LoadingFailCounter->Add(1);
    }
};

class TLoadTimer {
private:
    TLoadTimeSignals& Signals;
    TInstant Start;
    ui64& Duration;
    static ui64 DummyDuration;

public:
    TLoadTimer(TLoadTimeSignals& signals, ui64& duration)
        : Signals(signals)
        , Duration(duration)
    {
        Start = TInstant::Now();
    }

    TLoadTimer(TLoadTimeSignals& signals)
        : Signals(signals)
        , Duration(DummyDuration)
    {
        Start = TInstant::Now();
    }

    ~TLoadTimer() {
        TInstant finish = TInstant::Now();
        Signals.AddLoadingTime(Duration = (finish - Start).MicroSeconds());
    }
};

}

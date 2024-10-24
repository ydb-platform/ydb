#pragma once
#include "common/owner.h"

#include <ydb/library/actors/core/log.h>

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

class TLoadTimeSignals;

class TLoadTimeSignals: public TCommonCountersOwner {
public:
    class TLoadTimer : public TNonCopyable {
    private:
        const TLoadTimeSignals& Signals;
        TInstant Start;
        TString Name;
        bool Failed = false;

    public:
        TLoadTimer(const TLoadTimeSignals& signals, const TString& name)
            : Signals(signals)
            , Name(name)
        {
            Start = TInstant::Now();
        }

        void AddLoadingFail();

        ~TLoadTimer();
    };

private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr LoadingTimeCounter;
    NMonitoring::TDynamicCounters::TCounterPtr FailedLoadingTimeCounter;
    NMonitoring::TDynamicCounters::TCounterPtr LoadingFailCounter;
    TString Type;

public:
    TLoadTimeSignals(const TString& type)
        : TBase("Startup")
        , Type(type)
    {
        LoadingTimeCounter = TBase::GetValue("Startup/" + type + "LoadingTime");
        FailedLoadingTimeCounter = TBase::GetValue("Startup/" + type + "FailedLoadingTime");
        LoadingFailCounter = TBase::GetValue("Startup/" + type + "LoadingFailCount");
    }

    TLoadTimer StartGuard() const {
        return TLoadTimer(*this, Type + "LoadingTime");
    }

private:
    void AddLoadingTime(ui64 microSeconds) const {
        LoadingTimeCounter->Add(microSeconds);
    }

    void AddFailedLoadingTime(ui64 microSeconds) const {
        FailedLoadingTimeCounter->Add(microSeconds);
    }

    void AddLoadingFail() const {
        LoadingFailCounter->Add(1);
    }
};

class TTableLoadTimeCounters {
public:
    NColumnShard::TLoadTimeSignals TableLoadTimeCounters;
    NColumnShard::TLoadTimeSignals SchemaPresetLoadTimeCounters;
    NColumnShard::TLoadTimeSignals TableVersionsLoadTimeCounters;
    NColumnShard::TLoadTimeSignals SchemaPresetVersionsLoadTimeCounters;

public:
    TTableLoadTimeCounters()
    : TableLoadTimeCounters("Tables")
    , SchemaPresetLoadTimeCounters("SchemaPreset")
    , TableVersionsLoadTimeCounters("TableVersionss")
    , SchemaPresetVersionsLoadTimeCounters("SchemaPresetVersions")
    {
    }
};

}

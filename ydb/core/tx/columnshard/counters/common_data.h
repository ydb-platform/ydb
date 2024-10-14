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
    NMonitoring::TDynamicCounters::TCounterPtr TablesLoadingTimeCounter;
    NMonitoring::TDynamicCounters::TCounterPtr SchemaPresetLoadingTimeCounter;
    NMonitoring::TDynamicCounters::TCounterPtr TableVersionsLoadingTimeCounter;
    NMonitoring::TDynamicCounters::TCounterPtr SchemaPresetVersionsLoadingTimeCounter;

public:
    TLoadTimeSignals()
        : TBase("Startup")
    {
        TablesLoadingTimeCounter = TBase::GetDeriviative("Startup/TablesLoadingTime");;
        SchemaPresetLoadingTimeCounter = TBase::GetDeriviative("Startup/SchemaPresetLoadingTime");;
        TableVersionsLoadingTimeCounter = TBase::GetDeriviative("Startup/TableVersionsLoadingTime");;
        SchemaPresetVersionsLoadingTimeCounter = TBase::GetDeriviative("Startup/SchemaPreseVersionstLoadingTime");;
    }

    void SetTablesLoadingTime(ui64 microSeconds) {
        TablesLoadingTimeCounter->Set(microSeconds);
    }

    void SetSchemaPresetLoadingTime(ui64 microSeconds) {
        SchemaPresetLoadingTimeCounter->Set(microSeconds);
    }

    void SetTableVersionsLoadingTime(ui64 microSeconds) {
        TableVersionsLoadingTimeCounter->Set(microSeconds);
    }

    void SetSchemaPresetVersionsLoadingTime(ui64 microSeconds) {
        SchemaPresetVersionsLoadingTimeCounter->Set(microSeconds);
    }
};

}

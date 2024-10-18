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

    NMonitoring::TDynamicCounters::TCounterPtr TablesLoadingFailCounter;
    NMonitoring::TDynamicCounters::TCounterPtr SchemaPresetLoadingFailCounter;
    NMonitoring::TDynamicCounters::TCounterPtr TableVersionsLoadingFailCounter;
    NMonitoring::TDynamicCounters::TCounterPtr SchemaPresetVersionsLoadingFailCounter;


public:
    TLoadTimeSignals()
        : TBase("Startup")
    {
        TablesLoadingTimeCounter = TBase::GetValue("Startup/TablesLoadingTime");;
        SchemaPresetLoadingTimeCounter = TBase::GetValue("Startup/SchemaPresetLoadingTime");;
        TableVersionsLoadingTimeCounter = TBase::GetValue("Startup/TableVersionsLoadingTime");;
        SchemaPresetVersionsLoadingTimeCounter = TBase::GetValue("Startup/SchemaPreseVersionstLoadingTime");;

        TablesLoadingFailCounter = TBase::GetValue("Startup/TablesLoadingFailCount");;
        SchemaPresetLoadingFailCounter = TBase::GetValue("Startup/SchemaPresetLoadingFailCount");;
        TableVersionsLoadingFailCounter = TBase::GetValue("Startup/TableVersionsLoadingFailCount");;
        SchemaPresetVersionsLoadingFailCounter = TBase::GetValue("Startup/SchemaPreseVersionstLoadingFailCount");;
    }

    void AddTablesLoadingTime(ui64 microSeconds) {
        TablesLoadingTimeCounter->Add(microSeconds);
    }

    void AddSchemaPresetLoadingTime(ui64 microSeconds) {
        SchemaPresetLoadingTimeCounter->Add(microSeconds);
    }

    void AddTableVersionsLoadingTime(ui64 microSeconds) {
        TableVersionsLoadingTimeCounter->Add(microSeconds);
    }

    void AddSchemaPresetVersionsLoadingTime(ui64 microSeconds) {
        SchemaPresetVersionsLoadingTimeCounter->Add(microSeconds);
    }

    void AddLoadingTablesFail() {
        TablesLoadingFailCounter->Add(1);
    }

    void AddLoadingSchemaPresetFail() {
        SchemaPresetLoadingFailCounter->Add(1);
    }

    void AddLoadingTableVersionsFail() {
        TableVersionsLoadingFailCounter->Add(1);
    }

    void AddLoadingSchemaPresetVersionsFail() {
        SchemaPresetVersionsLoadingFailCounter->Add(1);
    }
};

}

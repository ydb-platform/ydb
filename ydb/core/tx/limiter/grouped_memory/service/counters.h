#pragma once
#include <ydb/core/tx/columnshard/counters/common/owner.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TStageCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr AllocatedBytes;
    NMonitoring::TDynamicCounters::TCounterPtr AllocatedChunks;
    NMonitoring::TDynamicCounters::TCounterPtr WaitingBytes;
    NMonitoring::TDynamicCounters::TCounterPtr WaitingChunks;

public:
    TStageCounters(const TCommonCountersOwner& owner, const TString& name)
        : TBase(owner, "stage", name)
        , AllocatedBytes(TBase::GetValue("Allocated/Bytes"))
        , AllocatedChunks(TBase::GetValue("Allocated/Count"))
        , WaitingBytes(TBase::GetValue("Waiting/Bytes"))
        , WaitingChunks(TBase::GetValue("Waiting/Count")) {
    }

    void Add(const ui64 volume, const bool allocated) {
        if (allocated) {
            AllocatedBytes->Add(volume);
            AllocatedChunks->Add(1);
        } else {
            WaitingBytes->Add(volume);
            WaitingChunks->Add(1);
        }
    }

    void Sub(const ui64 volume, const bool allocated) {
        if (allocated) {
            AllocatedBytes->Sub(volume);
            AllocatedChunks->Sub(1);
        } else {
            WaitingBytes->Sub(volume);
            WaitingChunks->Sub(1);
        }
    }
};

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

public:
    NMonitoring::TDynamicCounters::TCounterPtr GroupsCount;
    NMonitoring::TDynamicCounters::TCounterPtr ProcessesCount;
    TCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, const TString& name)
        : TBase(NColumnShard::TCommonCountersOwner("grouped_memory_limiter", counters), "limiter_name", name)
        , GroupsCount(TBase::GetValue("Groups/Count"))
        , ProcessesCount(TBase::GetValue("Processes/Count")) {
    }

    std::shared_ptr<TStageCounters> BuildStageCounters(const TString& stageName) const {
        return std::make_shared<TStageCounters>(*this, stageName);
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager

#pragma once
#include "common/owner.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

class TSubColumnsStat {
private:
    YDB_READONLY(ui64, Size, 0);
    YDB_READONLY(ui64, Count, 0);

public:
    TSubColumnsStat() = default;

    void Add(const ui64 size) {
        Size += size;
        ++Count;
    }
};

class TCategoryCommonCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr WriteBytes;
    NMonitoring::TDynamicCounters::TCounterPtr ReadBytes;
    NMonitoring::TDynamicCounters::TCounterPtr WriteCount;
    NMonitoring::TDynamicCounters::TCounterPtr ReadCount;

public:
    TCategoryCommonCounters(const TCommonCountersOwner& base)
        : TBase(base) {
        WriteBytes = TBase::GetDeriviative("Write/Bytes");
        ReadBytes = TBase::GetDeriviative("Read/Bytes");
        WriteCount = TBase::GetDeriviative("Write/Count");
        ReadCount = TBase::GetDeriviative("Read/Count");
    }

    void OnRead(const TSubColumnsStat& stat) const {
        ReadBytes->Add(stat.GetSize());
        ReadCount->Add(stat.GetCount());
    }

    void OnWrite(const TSubColumnsStat& stat) const {
        WriteBytes->Add(stat.GetSize());
        WriteCount->Add(stat.GetCount());
    }

    void OnRead(const ui64 size) const {
        ReadBytes->Add(size);
        ReadCount->Add(1);
    }
};

class TSubColumnCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    std::shared_ptr<TCategoryCommonCounters> Columns;
    std::shared_ptr<TCategoryCommonCounters> Others;

public:
    const TCategoryCommonCounters& GetColumnCounters() const {
        return *Columns;
    }

    const TCategoryCommonCounters& GetOtherCounters() const {
        return *Others;
    }

    TSubColumnCounters(const TCommonCountersOwner& base)
        : TBase(base)
        , Columns(std::make_shared<TCategoryCommonCounters>(base.CreateSubGroup("DataCategory", "Columns")))
        , Others(std::make_shared<TCategoryCommonCounters>(base.CreateSubGroup("DataCategory", "Others"))) {
    }
};

}   // namespace NKikimr::NColumnShard

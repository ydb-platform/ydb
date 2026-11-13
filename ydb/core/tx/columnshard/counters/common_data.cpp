#include "common_data.h"

namespace NKikimr::NColumnShard {

TDataOwnerSignals::TDataOwnerSignals(const TString& module, const TString dataName)
    : TBase(module)
    , DataName(dataName)
{
    DataSize = TBase::GetValueAutoAggregationsClient(DataName + "/Size");
    ChunksCount = TBase::GetValueAutoAggregationsClient(DataName + "/Chunks/Count");

    AddCount = GetDeriviative(DataName + "/Add/Count");
    AddBytes = GetDeriviative(DataName + "/Add/Bytes");
    EraseCount = GetDeriviative(DataName + "/Erase/Count");
    EraseBytes = GetDeriviative(DataName + "/Erase/Bytes");
    SkipAddCount = GetDeriviative(DataName + "/SkipAdd/Count");
    SkipAddBytes = GetDeriviative(DataName + "/SkipAdd/Bytes");
    SkipEraseCount = GetDeriviative(DataName + "/SkipErase/Count");
    SkipEraseBytes = GetDeriviative(DataName + "/SkipErase/Bytes");
}

TLoadTimeSignals::TLoadTimer::~TLoadTimer() {
    const TDuration duration = TMonotonic::Now() - Start;
    if (Failed) {
        Signals.AddFailedLoadingTime(duration.MicroSeconds());
    } else {
        Signals.AddLoadingTime(duration.MicroSeconds());
    }
    if (Total) {
        *Total += duration;
    }
}

void TLoadTimeSignals::TLoadTimer::AddLoadingFail() {
    if (!Failed) {
        Failed = true;
        Signals.AddLoadingFail();
    }
}

}   // namespace NKikimr::NColumnShard

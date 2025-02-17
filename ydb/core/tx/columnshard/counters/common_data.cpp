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
    ui64 duration = (TInstant::Now() - Start).MicroSeconds();
    if (Failed) {
        Signals.AddFailedLoadingTime(duration);
    } else {
        Signals.AddLoadingTime(duration);
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)(Name, duration);
}

void TLoadTimeSignals::TLoadTimer::AddLoadingFail() {
    if (!Failed) {
        Failed = true;
        Signals.AddLoadingFail();
    }
}

}

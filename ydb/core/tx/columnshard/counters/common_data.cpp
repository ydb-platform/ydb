#include "common_data.h"

namespace NKikimr::NColumnShard {

TDataOwnerSignals::TDataOwnerSignals(const TString& module, const TString dataName)
    : TBase(module)
    , DataName(dataName)
{
    AddCount = GetDeriviative(DataName + "/Add/Count");
    AddBytes = GetDeriviative(DataName + "/Add/Bytes");
    EraseCount = GetDeriviative(DataName + "/Erase/Count");
    EraseBytes = GetDeriviative(DataName + "/Erase/Bytes");
    SkipAddCount = GetDeriviative(DataName + "/SkipAdd/Count");
    SkipAddBytes = GetDeriviative(DataName + "/SkipAdd/Bytes");
    SkipEraseCount = GetDeriviative(DataName + "/SkipErase/Count");
    SkipEraseBytes = GetDeriviative(DataName + "/SkipErase/Bytes");
}

}

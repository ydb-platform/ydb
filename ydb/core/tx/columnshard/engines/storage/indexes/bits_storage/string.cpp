#include "string.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes {

bool TFixStringBitsStorage::DoGet(const ui32 idx) const {
    AFL_VERIFY(idx < Data.size() * 8);
    const ui8 start = (*(ui8*)&Data[idx / 8]);
    return start & (1 << (idx % 8));
}

NKikimr::TConclusionStatus TFixStringBitsStorage::DoDeserializeFromString(const TString& data) {
    Data = data;
    return TConclusionStatus::Success();
}

TFixStringBitsStorage::TFixStringBitsStorage(const TDynBitMap& bitsVector) {
    AFL_VERIFY(bitsVector.Size() % 8 == 0);
    Data.resize(bitsVector.Size() / 8, '\0');
    ui32 byteIdx = 0;
    ui8 byteCurrent = 0;
    ui8 shiftCurrent = 1;
    for (ui32 i = 0; i < bitsVector.Size(); ++i) {
        if (i && i % 8 == 0) {
            Data[byteIdx] = (char)byteCurrent;
            byteCurrent = 0;
            shiftCurrent = 1;
            ++byteIdx;
        }
        if (bitsVector[i]) {
            byteCurrent += shiftCurrent;
        }
        shiftCurrent = (shiftCurrent << 1);
    }
    if (byteCurrent) {
        Data[byteIdx] = (char)byteCurrent;
    }
    AFL_VERIFY(byteIdx + 1 == Data.size())("idx", byteIdx)("data", Data.size());
}

}   // namespace NKikimr::NOlap::NIndexes

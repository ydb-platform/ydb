#include "bits_storage.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes {

TString TFixStringBitsStorage::DebugString() const {
    TStringBuilder sb;
    ui32 count1 = 0;
    ui32 count0 = 0;
    for (ui32 i = 0; i < Bits.Size(); ++i) {
        //            if (i % 20 == 0 && i) {
        //                sb << i << " ";
        //            }
        if (Get(i)) {
            //                sb << 1 << " ";
            ++count1;
        } else {
            //                sb << 0 << " ";
            ++count0;
        }
    }
    sb << Bits.Size() << "=" << count0 << "[0]+" << count1 << "[1]";
    return sb;
}

bool TFixStringBitsStorage::Get(const ui32 idx) const {
    return Bits.Test(idx);
}

TFixStringBitsStorage::TFixStringBitsStorage(const TString& data) noexcept {
    try {
        TStringInput input(data);
        Bits.Load(&input);
    } catch (...) {
        Bits.Reserve(1024);
    }
}

TString TFixStringBitsStorage::SerializeToString() const {
    TString result;
    result.reserve(Bits.GetChunkCount() * sizeof(TDynBitMap::TChunk) + 16);
    TStringOutput output(result);
    Bits.Save(&output);
    return result;
}

}   // namespace NKikimr::NOlap::NIndexes

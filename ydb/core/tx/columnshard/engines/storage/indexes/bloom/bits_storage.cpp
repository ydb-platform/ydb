#include "bits_storage.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes {

TString TFixStringBitsStorage::DebugString() const {
    TStringBuilder sb;
    ui32 count1 = 0;
    ui32 count0 = 0;
    for (ui32 i = 0; i < GetSizeBits(); ++i) {
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
    sb << GetSizeBits() << "=" << count0 << "[0]+" << count1 << "[1]";
    return sb;
}

void TFixStringBitsStorage::Set(const bool val, const ui32 idx) {
    AFL_VERIFY(idx < GetSizeBits());
    auto* start = &Data[idx / 8];
    ui8 word = (*(ui8*)start);
    if (val) {
        word |= 1 << (idx % 8);
    } else {
        word &= (Max<ui8>() - (1 << (idx % 8)));
    }
    memcpy(start, &word, sizeof(ui8));
}

bool TFixStringBitsStorage::Get(const ui32 idx) const {
    AFL_VERIFY(idx < GetSizeBits());
    const ui8 start = (*(ui8*)&Data[idx / 8]);
    return start & (1 << (idx % 8));
}

}   // namespace NKikimr::NOlap::NIndexes

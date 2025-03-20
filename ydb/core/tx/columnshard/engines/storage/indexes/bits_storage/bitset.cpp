#include "bitset.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes {

bool TBitSetStorage::DoGet(const ui32 idx) const {
    return Bits.Test(idx);
}

TConclusionStatus TBitSetStorage::DoDeserializeFromString(const TString& data) {
    TStringInput input(data);
    Bits.Load(&input);
    return TConclusionStatus::Success();
}

TString TBitSetStorage::DoSerializeToString() const {
    TString result;
    result.reserve(Bits.GetChunkCount() * sizeof(TDynBitMap::TChunk) + 16);
    TStringOutput output(result);
    Bits.Save(&output);
    return result;
}

}   // namespace NKikimr::NOlap::NIndexes

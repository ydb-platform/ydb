#include "bitset.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes {

TString TBitSetStorageConstructor::DoSerializeToString(TDynBitMap&& bm) const {
    TString result;
    result.reserve(bm.GetChunkCount() * sizeof(TDynBitMap::TChunk) + 16);
    TStringOutput output(result);
    bm.Save(&output);
    return result;
}

std::shared_ptr<IBitsStorageViewer> TBitSetStorageConstructor::DoRestore(const TString& data) const {
    TStringInput input(data);
    TDynBitMap bitmap;
    bitmap.Load(&input);
    return std::make_shared<TBitSetStorage>(std::move(bitmap));
}

bool TBitSetStorage::DoGet(const ui32 idx) const {
    return Bits.Test(idx);
}

}   // namespace NKikimr::NOlap::NIndexes

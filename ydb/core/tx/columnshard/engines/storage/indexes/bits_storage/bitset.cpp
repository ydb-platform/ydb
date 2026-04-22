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

TConclusion<std::shared_ptr<IBitsStorageViewer>> TBitSetStorageConstructor::DoRestore(const TString& data) const {
    try {
        TStringInput input(data);
        TDynBitMap bitmap;
        bitmap.Load(&input);
        return std::make_shared<TBitSetStorage>(std::move(bitmap));
    } catch(...) {
        return TConclusionStatus::Fail("cannot deserialize bitset index: " + CurrentExceptionMessage());
    }
}

bool TBitSetStorage::DoGet(const ui32 idx) const {
    return Bits.Test(idx);
}

}   // namespace NKikimr::NOlap::NIndexes

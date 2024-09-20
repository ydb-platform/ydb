#include "data.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor.h>

namespace NKikimr::NOlap::NChunks {

void TPortionIndexChunk::DoAddIntoPortionBeforeBlob(const TBlobRangeLink16& bRange, TPortionInfoConstructor& portionInfo) const {
    AFL_VERIFY(!bRange.IsValid());
    portionInfo.AddIndex(TIndexChunk(GetEntityId(), GetChunkIdxVerified(), RecordsCount, RawBytes, bRange));
}

std::shared_ptr<IPortionDataChunk> TPortionIndexChunk::DoCopyWithAnotherBlob(
    TString&& data, const TSimpleColumnInfo& /*columnInfo*/) const {
    return std::make_shared<TPortionIndexChunk>(GetChunkAddressVerified(), RecordsCount, RawBytes, std::move(data));
}

void TPortionIndexChunk::DoAddInplaceIntoPortion(TPortionInfoConstructor& portionInfo) const {
    portionInfo.AddIndex(TIndexChunk(GetEntityId(), GetChunkIdxVerified(), RecordsCount, RawBytes, GetData()));
}

}   // namespace NKikimr::NOlap::NIndexes
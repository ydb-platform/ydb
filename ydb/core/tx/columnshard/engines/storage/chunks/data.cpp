#include "data.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap::NChunks {

void TPortionIndexChunk::DoAddIntoPortionBeforeBlob(const TBlobRangeLink16& bRange, TPortionInfo& portionInfo) const {
    AFL_VERIFY(!bRange.IsValid());
    portionInfo.AddIndex(TIndexChunk(GetEntityId(), GetChunkIdxVerified(), RecordsCount, RawBytes, bRange));
}

}   // namespace NKikimr::NOlap::NIndexes
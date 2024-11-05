#include "chunks.h"
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor.h>

namespace NKikimr::NOlap {

std::vector<std::shared_ptr<IPortionDataChunk>> IPortionColumnChunk::DoInternalSplit(const TColumnSaver& saver, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, const std::vector<ui64>& splitSizes) const {
    ui64 sumSize = 0;
    for (auto&& i : splitSizes) {
        sumSize += i;
    }
    Y_ABORT_UNLESS(sumSize <= GetPackedSize());
    if (sumSize < GetPackedSize()) {
        Y_ABORT_UNLESS(GetRecordsCount() >= splitSizes.size() + 1);
    } else {
        Y_ABORT_UNLESS(GetRecordsCount() >= splitSizes.size());
    }
    auto result = DoInternalSplitImpl(saver, counters, splitSizes);
    if (sumSize == GetPackedSize()) {
        Y_ABORT_UNLESS(result.size() == splitSizes.size());
    } else {
        Y_ABORT_UNLESS(result.size() == splitSizes.size() + 1);
    }
    return result;
}

void IPortionColumnChunk::DoAddIntoPortionBeforeBlob(const TBlobRangeLink16& bRange, TPortionInfoConstructor& portionInfo) const {
    AFL_VERIFY(!bRange.IsValid());
    TColumnRecord rec(GetChunkAddressVerified(), bRange, BuildSimpleChunkMeta());
    portionInfo.AppendOneChunkColumn(std::move(rec));
}

}

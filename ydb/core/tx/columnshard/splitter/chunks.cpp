#include "chunks.h"

namespace NKikimr::NOlap {

std::vector<NKikimr::NOlap::IPortionColumnChunk::TPtr> IPortionColumnChunk::InternalSplit(const TColumnSaver& saver, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const std::vector<ui64>& splitSizes) const {
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
    auto result = DoInternalSplit(saver, counters, splitSizes);
    if (sumSize == GetPackedSize()) {
        Y_ABORT_UNLESS(result.size() == splitSizes.size());
    } else {
        Y_ABORT_UNLESS(result.size() == splitSizes.size() + 1);
    }
    return result;
}

}

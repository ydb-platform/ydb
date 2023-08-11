#include "chunks.h"

namespace NKikimr::NOlap {

std::vector<TSplittedColumnChunk> TSplittedColumnChunk::InternalSplit(const TColumnSaver& saver, std::shared_ptr<NColumnShard::TSplitterCounters> counters) {
    auto chunks = TSimpleSplitter(saver, counters).SplitBySizes(Data.GetSlicedBatch(), Data.GetSerializedChunk(), SplitSizes);
    Y_VERIFY(chunks.size() == SplitSizes.size() + 1);
    std::vector<TSplittedColumnChunk> newChunks;
    for (auto&& i : chunks) {
        newChunks.emplace_back(TSplittedColumnChunk(ColumnId, i));
    }
    SplitSizes.clear();
    return newChunks;
}

}

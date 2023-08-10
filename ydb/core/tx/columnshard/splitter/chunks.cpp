#include "chunks.h"

namespace NKikimr::NOlap {

std::vector<TSplittedColumnChunk> TSplittedColumnChunk::InternalSplit(const TColumnSaver& saver) {
    auto chunks = TSimpleSplitter(saver).SplitBySizes(Data.GetSlicedBatch(), Data.GetSerializedChunk(), SplitSizes);
    Y_VERIFY(chunks.size() == SplitSizes.size() + 1);
    std::vector<TSplittedColumnChunk> newChunks;
    for (auto&& i : chunks) {
        newChunks.emplace_back(TSplittedColumnChunk(ColumnId, i));
    }
    SplitSizes.clear();
    return newChunks;
}

}

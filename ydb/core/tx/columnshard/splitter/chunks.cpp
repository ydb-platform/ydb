#include "chunks.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>

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

TString TSimpleOrderedColumnChunk::DebugString() const {
    return TStringBuilder() << "address=" << ChunkAddress.DebugString() << ";data_size=" << Data.size() << ";";
}

TString TOrderedColumnChunk::DebugString() const {
    return TStringBuilder() << TBase::DebugString() << "records_count=" << Column->length() << ";data=" << NArrow::DebugJson(Column, 3, 3) << ";";
}

}

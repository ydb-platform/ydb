#include "column.h"
#include <ydb/core/tx/columnshard/splitter/simple.h>

namespace NKikimr::NOlap::NChunks {

std::vector<std::shared_ptr<IPortionDataChunk>> TChunkPreparation::DoInternalSplitImpl(const TColumnSaver& saver, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, const std::vector<ui64>& splitSizes) const {
    auto rb = NArrow::TStatusValidator::GetValid(ColumnInfo.GetLoader()->Apply(Data));

    auto chunks = TSimpleSplitter(saver, counters).SplitBySizes(rb, Data, splitSizes);
    std::vector<std::shared_ptr<IPortionDataChunk>> newChunks;
    for (auto&& i : chunks) {
        Y_ABORT_UNLESS(i.GetSlicedBatch()->num_columns() == 1);
        newChunks.emplace_back(std::make_shared<TChunkPreparation>(saver.Apply(i.GetSlicedBatch()), i.GetSlicedBatch()->column(0), TChunkAddress(GetColumnId(), GetChunkIdxOptional().value_or(0)), ColumnInfo));
    }
    return newChunks;
}

}

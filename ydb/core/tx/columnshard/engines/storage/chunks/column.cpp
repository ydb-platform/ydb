#include "column.h"
#include <ydb/core/formats/arrow/splitter/simple.h>

namespace NKikimr::NOlap::NChunks {

std::vector<std::shared_ptr<IPortionDataChunk>> TChunkPreparation::DoInternalSplitImpl(
    const TColumnSaver& /*saver*/, const std::shared_ptr<NColumnShard::TSplitterCounters>& /*counters*/, const std::vector<ui64>& splitSizes) const {
    auto accessor = ColumnInfo.GetLoader()->ApplyVerified(Data, GetRecordsCountVerified());
    const auto predSaver = [&](const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& arr) {
        return ColumnInfo.GetLoader()->GetAccessorConstructor().SerializeToString(arr, ColumnInfo.GetLoader()->BuildAccessorContext(arr->GetRecordsCount()));
    };
    std::vector<NArrow::NAccessor::TChunkedArraySerialized> chunks = accessor->SplitBySizes(predSaver, Data, splitSizes);

    std::vector<std::shared_ptr<IPortionDataChunk>> newChunks;
    for (auto&& i : chunks) {
        newChunks.emplace_back(std::make_shared<TChunkPreparation>(
            i.GetSerializedData(), i.GetArray(), TChunkAddress(GetColumnId(), GetChunkIdxOptional().value_or(0)), ColumnInfo));
    }

    return newChunks;
}

}   // namespace NKikimr::NOlap::NChunks

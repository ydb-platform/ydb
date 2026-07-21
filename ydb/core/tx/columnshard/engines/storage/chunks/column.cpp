#include "chunked_array_serialized.h"
#include "column.h"

#include <ydb/core/formats/arrow/accessor/common/const.h>
#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/splitter/simple.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_accessor.h>

namespace NKikimr::NOlap::NChunks {

void TChunkPreparation::DoAddIntoPortionBeforeBlob(const TBlobRangeLink16& bRange, TPortionAccessorConstructor& portionInfo) const {
    AFL_VERIFY(!bRange.IsValid());
    TChunkMeta metaCopy = Record.GetMeta();
    TColumnRecord rec(GetChunkAddressVerified(), bRange, std::move(metaCopy));
    portionInfo.AppendOneChunkColumn(std::move(rec));
}

std::vector<std::shared_ptr<IPortionDataChunk>> TChunkPreparation::DoInternalSplitImpl(const TColumnSaver& /*saver*/,
    const std::shared_ptr<NColumnShard::TSplitterCounters>& /*counters*/, const std::vector<ui64>& splitSizes) const {
    auto additionalData = Record.GetMeta().GetAdditionalAccessorData();
    auto accessor = ColumnInfo.GetLoader()->ApplyVerified(Data, GetRecordsCountVerified(), std::nullopt, std::move(additionalData));

    const auto predSaver = [&](const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& arr) {
        return ColumnInfo.GetLoader()->GetAccessorConstructor().SerializeToBlobAndMeta(
            arr, ColumnInfo.GetLoader()->BuildAccessorContext(arr->GetRecordsCount()));
    };
    std::vector<TChunkedArraySerialized> chunks = SplitBySizes(*accessor, predSaver, Data, splitSizes);

    std::vector<std::shared_ptr<IPortionDataChunk>> newChunks;
    const ui16 baseChunkIdx = GetChunkIdxOptional().value_or(0);
    for (size_t i = 0; i < chunks.size(); ++i) {
        newChunks.emplace_back(std::make_shared<TChunkPreparation>(chunks[i].GetSerializedData(), chunks[i].GetArray(),
            TChunkAddress(GetColumnId(), baseChunkIdx + i), ColumnInfo, chunks[i].GetMeta()));
    }

    return newChunks;
}

}   // namespace NKikimr::NOlap::NChunks

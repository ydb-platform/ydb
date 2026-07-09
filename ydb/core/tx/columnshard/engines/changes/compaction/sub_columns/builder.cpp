#include "builder.h"

#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>

namespace NKikimr::NOlap::NCompaction::NSubColumns {

std::shared_ptr<NArrow::NAccessor::IChunkedArray> TMergedBuilder::MaybeDictionaryEncode(
    const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& accessor, const ui32 filledRecordsCount) const {
    const auto enumerateNotNull = [&accessor](const auto& consumer) {
        auto chunked = accessor->GetChunkedArray();
        for (int c = 0; c < chunked->num_chunks(); ++c) {
            const auto* binary = dynamic_cast<const arrow::BinaryArray*>(chunked->chunk(c).get());
            if (!binary) {
                continue;
            }
            for (int64_t i = 0; i < binary->length(); ++i) {
                if (binary->IsNull(i)) {
                    continue;
                }
                auto view = binary->GetView(i);
                if (!consumer(TStringBuf(view.data(), view.size()))) {
                    return;
                }
            }
        }
    };
    if (!Settings.IsDictionary(filledRecordsCount, enumerateNotNull)) {
        return accessor;
    }
    const NArrow::NAccessor::TChunkConstructionData cData(
        accessor->GetRecordsCount(), nullptr, arrow::binary(), NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer());
    return NArrow::NAccessor::NDictionary::TConstructor().Construct(accessor, cData).DetachResult();
}

TColumnPortionResult TMergedBuilder::Finish(const TColumnMergeContext& cmContext) {
    if (RecordIndex) {
        FlushData();
    }
    std::vector<TColumnPortionResult> portions;
    IColumnMerger::TPortionColumnChunkWriter<NArrow::NAccessor::TSubColumnsArray, NArrow::NAccessor::NSubColumns::TConstructor> pColumn(
        NArrow::NAccessor::NSubColumns::TConstructor(), cmContext.GetColumnId());
    AFL_VERIFY(Result.size());
    for (auto&& p : Result) {
        pColumn.AddChunk(p, cmContext);
    }
    return std::move(pColumn);
}

void TMergedBuilder::FlushData() {
    AFL_VERIFY(RecordIndex);
    auto portionOthersData = OthersBuilder->Finish(Remapper.BuildRemapInfo(OthersBuilder->GetStatsByKeyIndex(), Settings, RecordIndex));
    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> arrays;
    TDictStats::TBuilder statsBuilder;
    for (ui32 idx = 0; idx < ColumnBuilders.size(); ++idx) {
        if (ColumnBuilders[idx].GetFilledRecordsCount()) {
            auto accessor = ColumnBuilders[idx].Finish(RecordIndex);
            accessor = MaybeDictionaryEncode(accessor, ColumnBuilders[idx].GetFilledRecordsCount());
            statsBuilder.Add(ResultColumnStats.GetColumnName(idx), ColumnBuilders[idx].GetFilledRecordsCount(),
                ColumnBuilders[idx].GetFilledRecordsSize(), accessor->GetType());
            arrays.emplace_back(std::move(accessor));
        }
    }
    auto stats = statsBuilder.Finish();
    TColumnsData cData(stats, std::make_shared<NArrow::TGeneralContainer>(stats.BuildColumnsSchema()->fields(), std::move(arrays)));
    Result.emplace_back(
        std::make_shared<TSubColumnsArray>(std::move(cData), std::move(portionOthersData), arrow::binary(), RecordIndex, Settings));
    Initialize();
}

void TMergedBuilder::Initialize() {
    ColumnBuilders.clear();
    for (ui32 i = 0; i < ResultColumnStats.GetColumnsCount(); ++i) {
        switch (ResultColumnStats.GetAccessorType(i)) {
            case NArrow::NAccessor::IChunkedArray::EType::Array:
                ColumnBuilders.emplace_back(TPlainBuilder(0, 0));
                break;
            case NArrow::NAccessor::IChunkedArray::EType::SparsedArray:
                ColumnBuilders.emplace_back(TSparsedBuilder(nullptr, 0, 0));
                break;
            case NArrow::NAccessor::IChunkedArray::EType::Undefined:
            case NArrow::NAccessor::IChunkedArray::EType::SerializedChunkedArray:
            case NArrow::NAccessor::IChunkedArray::EType::CompositeChunkedArray:
            case NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray:
            case NArrow::NAccessor::IChunkedArray::EType::SubColumnsPartialArray:
            case NArrow::NAccessor::IChunkedArray::EType::ChunkedArray:
            // Dictionary is never planned here by construction: GetAccessorType() defers it to
            // MaybeDictionaryEncode after materialization, so the plan only yields Array/Sparsed.
            case NArrow::NAccessor::IChunkedArray::EType::Dictionary:
                AFL_VERIFY(false);
        }
    }
    OthersBuilder = TOthersData::MakeMergedBuilder();
    RecordIndex = 0;
    SumValuesSize = 0;
}

void TMergedBuilder::FinishRecord() {
    Y_UNUSED(Context);
    ++RecordIndex;
    if (SumValuesSize >= Settings.GetChunkMemoryLimit()) {
        FlushData();
    }
}

}   // namespace NKikimr::NOlap::NCompaction::NSubColumns

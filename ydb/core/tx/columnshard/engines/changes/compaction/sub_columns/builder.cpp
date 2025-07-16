#include "builder.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>

namespace NKikimr::NOlap::NCompaction::NSubColumns {

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
            statsBuilder.Add(ResultColumnStats.GetColumnName(idx), ColumnBuilders[idx].GetFilledRecordsCount(),
                ColumnBuilders[idx].GetFilledRecordsSize(), ResultColumnStats.GetAccessorType(idx));
            arrays.emplace_back(ColumnBuilders[idx].Finish(RecordIndex));
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

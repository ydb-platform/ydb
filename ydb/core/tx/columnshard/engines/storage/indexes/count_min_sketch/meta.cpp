#include "meta.h"
#include "checker.h"
#include <ydb/core/formats/arrow/hash/xx_hash.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/library/minsketch/count_min_sketch.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes {

std::shared_ptr<arrow::RecordBatch> TCountMinSketchIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader) const {
    std::vector<std::unique_ptr<TCountMinSketch>> sketchesByColumns;
    sketchesByColumns.reserve(ColumnIds.size());
    for (const auto& _ : ColumnIds) {
        sketchesByColumns.emplace_back(std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create()));
    }

    AFL_VERIFY(std::distance(reader.begin(), reader.end()) == static_cast<long>(sketchesByColumns.size()));

    for (reader.Start(); reader.IsCorrect(); reader.ReadNext()) {
        size_t sketchIndex = 0;
        for (auto&& colReader : reader) {
            auto array = colReader.GetCurrentChunk();
            auto& sketch = sketchesByColumns[sketchIndex];
            int i = colReader.GetCurrentRecordIndex();

            NArrow::SwitchType(array->type_id(), [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

                const TArray& arrTyped = static_cast<const TArray&>(*array);
                if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                    auto cell = TCell::Make(arrTyped.Value(i));
                    sketch->Count(cell.Data(), cell.Size());
                    return true;
                }
                if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                    auto view = arrTyped.GetView(i);
                    sketch->Count(view.data(), view.size());
                    return true;
                }
                AFL_VERIFY(false);
            });
            ++sketchIndex;
        }
    }

    std::vector<std::shared_ptr<arrow::Array>> resultColumns(ColumnIds.size());
    size_t resultIndex = 0;
    for (const auto& sketch : sketchesByColumns) {
        arrow::UInt32Builder builder;
        NArrow::TStatusValidator::Validate(builder.Reserve(Width * Depth));
        const auto sketchData = reinterpret_cast<const ui32*>(sketch.get() + 1);
        NArrow::TStatusValidator::Validate(builder.AppendValues(sketchData, Width * Depth));
        NArrow::TStatusValidator::Validate(builder.Finish(&resultColumns[resultIndex]));
        ++resultIndex;
    }

    return arrow::RecordBatch::Make(ResultSchema, Width * Depth, resultColumns);
}

void TCountMinSketchIndexMeta::DoFillIndexCheckers(const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const {
    for (auto&& branch : info->GetBranches()) {
        branch->MutableIndexes().emplace_back(std::make_shared<TCountMinSketchChecker>(GetIndexId()));
    }
}

}   // namespace NKikimr::NOlap::NIndexes

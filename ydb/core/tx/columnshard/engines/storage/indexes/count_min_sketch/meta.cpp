#include "meta.h"
#include "checker.h"
#include <ydb/core/formats/arrow/hash/xx_hash.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/library/minsketch/stack_count_min_sketch.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes::NCountMinSketch {

using TCountMinSketch = TStackAllocatedCountMinSketch<256, 8>;

TString TIndexMeta::DoBuildIndexImpl(std::vector<TChunkedColumnReader>&& columnReaders) const {
    AFL_VERIFY(columnReaders.size() == ColumnIds.size());

    std::vector<TCountMinSketch> sketchesByColumns;
    sketchesByColumns.reserve(ColumnIds.size());

    for (auto&& colReader : columnReaders) {
        sketchesByColumns.emplace_back();
        auto& sketch = sketchesByColumns.back();

        for (colReader.Start(); colReader.IsCorrect(); colReader.ReadNext()) {
            auto array = colReader.GetCurrentChunk();
            int i = colReader.GetCurrentRecordIndex();

            NArrow::SwitchType(array->type_id(), [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

                const TArray& arrTyped = static_cast<const TArray&>(*array);
                if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                    auto cell = TCell::Make(arrTyped.Value(i));
                    sketch.Count(cell.Data(), cell.Size());
                    return true;
                }
                if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                    auto view = arrTyped.GetView(i);
                    sketch.Count(view.data(), view.size());
                    return true;
                }
                AFL_VERIFY(false);
                return false;
            });
        }
    }

    TString result(reinterpret_cast<const char*>(sketchesByColumns.data()), sketchesByColumns.size() * TCountMinSketch::GetSize());
    return result;
}

void TIndexMeta::DoFillIndexCheckers(const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& /*schema*/) const {
    for (auto&& branch : info->GetBranches()) {
        branch->MutableIndexes().emplace_back(std::make_shared<TCountMinSketchChecker>(GetIndexId()));
    }
}

}   // namespace NKikimr::NOlap::NIndexes

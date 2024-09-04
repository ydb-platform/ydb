#include "meta.h"
#include "checker.h"
#include <ydb/core/formats/arrow/hash/xx_hash.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/library/minsketch/count_min_sketch.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes::NCountMinSketch {

TString TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader) const {
    auto sketch = std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create());

    for (auto& colReader : reader) {
        for (colReader.Start(); colReader.IsCorrect(); colReader.ReadNextChunk()) {
            auto array = colReader.GetCurrentChunk();

            NArrow::SwitchType(array->type_id(), [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

                const TArray& arrTyped = static_cast<const TArray&>(*array);
                if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                    for (int64_t i = 0; i < arrTyped.length(); ++i) {
                        auto cell = TCell::Make(arrTyped.Value(i));
                        sketch->Count(cell.Data(), cell.Size());
                    }
                    return true;
                }
                if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                    for (int64_t i = 0; i < arrTyped.length(); ++i) {
                        auto view = arrTyped.GetView(i);
                        sketch->Count(view.data(), view.size());
                    }
                    return true;
                }
                AFL_VERIFY(false)("message", "Unsupported arrow type for building an index");
                return false;
            });
        }
    }

    TString result(sketch->AsStringBuf());
    return result;
}

void TIndexMeta::DoFillIndexCheckers(const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& /*schema*/) const {
    for (auto&& branch : info->GetBranches()) {
        branch->MutableIndexes().emplace_back(std::make_shared<TCountMinSketchChecker>(GetIndexId()));
    }
}

}   // namespace NKikimr::NOlap::NIndexes

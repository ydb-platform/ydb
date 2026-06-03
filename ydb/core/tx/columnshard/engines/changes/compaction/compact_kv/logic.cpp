#include "logic.h"

#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/concatenate.h>

namespace NKikimr::NOlap::NCompaction {

void TCompactKVMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& /*mergeContext*/) {
    Sources.reserve(input.size());
    for (auto&& p : input) {
        if (!p) {
            Sources.emplace_back(nullptr);
            continue;
        }
        // The input may be a lazy TDeserializeChunkedArray, so materialize it
        // chunk-by-chunk via GetChunk (the only path that triggers deserialization).
        arrow::ArrayVector chunks;
        std::optional<NArrow::NAccessor::IChunkedArray::TFullDataAddress> address;
        for (ui32 position = 0; position < p->GetRecordsCount();) {
            address = p->GetChunk(address, position);
            chunks.emplace_back(address->GetArray());
            position += address->GetArray()->length();
        }
        std::shared_ptr<arrow::Array> arr =
            (chunks.size() == 1) ? chunks.front() : NArrow::TStatusValidator::GetValid(arrow::Concatenate(chunks));
        AFL_VERIFY(arr->type_id() == arrow::Type::BINARY)("type", arr->type()->ToString());
        Sources.emplace_back(std::static_pointer_cast<arrow::BinaryArray>(arr));
    }
}

TColumnPortionResult TCompactKVMerger::DoExecute(const TChunkMergeContext& chunkContext, TMergingContext& /*mergeContext*/) {
    const ui32 recordsCount = chunkContext.GetRemapper().GetRecordsCount();

    arrow::BinaryBuilder builder;
    NArrow::TStatusValidator::Validate(builder.Reserve(recordsCount));
    for (ui32 idx = 0; idx < recordsCount; ++idx) {
        const ui16 portionIdx = chunkContext.GetRemapper().GetIdxArray().Value(idx);
        const ui32 recordIdx = chunkContext.GetRemapper().GetRecordIdxArray().Value(idx);
        AFL_VERIFY(portionIdx < Sources.size());
        const auto& src = Sources[portionIdx];
        if (!src || src->IsNull(recordIdx)) {
            NArrow::TStatusValidator::Validate(builder.AppendNull());
        } else {
            const auto view = src->GetView(recordIdx);
            NArrow::TStatusValidator::Validate(builder.Append(view.data(), view.size()));
        }
    }
    std::shared_ptr<arrow::Array> resultArray;
    NArrow::TStatusValidator::Validate(builder.Finish(&resultArray));

    auto compactArr = std::make_shared<NArrow::NAccessor::TCompactKVArray>(resultArray);
    auto accContext = Context.GetLoader()->BuildAccessorContext(resultArray->length());
    TString blob = Context.GetLoader()->GetAccessorConstructor().SerializeToString(compactArr, accContext);

    TColumnPortionResult result(Context.GetColumnId());
    result.AddPreparedChunk(std::make_shared<NChunks::TChunkPreparation>(std::move(blob), compactArr,
        TChunkAddress(Context.GetColumnId(), 0), Context.GetIndexInfo().GetColumnFeaturesVerified(Context.GetColumnId())));
    return result;
}

}   // namespace NKikimr::NOlap::NCompaction

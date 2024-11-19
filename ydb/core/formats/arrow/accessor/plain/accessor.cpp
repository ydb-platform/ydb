#include "accessor.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/splitter/simple.h>

namespace NKikimr::NArrow::NAccessor {

std::optional<ui64> TTrivialArray::DoGetRawSize() const {
    return NArrow::GetArrayDataSize(Array);
}

std::vector<NKikimr::NArrow::NAccessor::TChunkedArraySerialized> TTrivialArray::DoSplitBySizes(
    const TColumnSaver& saver, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) {
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("f", GetDataType()) }));
    auto chunks = NArrow::NSplitter::TSimpleSplitter(saver).SplitBySizes(
        arrow::RecordBatch::Make(schema, GetRecordsCount(), { Array }), fullSerializedData, splitSizes);
    std::vector<TChunkedArraySerialized> result;
    for (auto&& i : chunks) {
        AFL_VERIFY(i.GetSlicedBatch()->num_columns() == 1);
        result.emplace_back(std::make_shared<TTrivialArray>(i.GetSlicedBatch()->column(0)), i.GetSerializedChunk());
    }
    return result;
}

std::shared_ptr<arrow::Scalar> TTrivialArray::DoGetMaxScalar() const {
    auto minMaxPos = NArrow::FindMinMaxPosition(Array);
    return NArrow::TStatusValidator::GetValid(Array->GetScalar(minMaxPos.second));
}

namespace {
class TChunkAccessor {
private:
    std::shared_ptr<arrow::ChunkedArray> ChunkedArray;
    std::optional<IChunkedArray::TLocalDataAddress>* Result;

public:
    TChunkAccessor(const std::shared_ptr<arrow::ChunkedArray>& chunkedArray, std::optional<IChunkedArray::TLocalDataAddress>& result)
        : ChunkedArray(chunkedArray)
        , Result(&result) {
    }
    ui64 GetChunksCount() const {
        return (ui64)ChunkedArray->num_chunks();
    }
    ui64 GetChunkLength(const ui32 idx) const {
        return (ui64)ChunkedArray->chunk(idx)->length();
    }
    void OnArray(const ui32 idx, const ui32 startPosition) const {
        const auto& arr = ChunkedArray->chunk(idx);
        *Result = IChunkedArray::TLocalDataAddress(arr, startPosition, idx);
    }
};

}   // namespace

IChunkedArray::TLocalDataAddress TTrivialChunkedArray::DoGetLocalData(
    const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const {
    std::optional<IChunkedArray::TLocalDataAddress> result;
    TChunkAccessor accessor(Array, result);
    SelectChunk(chunkCurrent, position, accessor);
    AFL_VERIFY(result);
    return *result;
}

std::optional<ui64> TTrivialChunkedArray::DoGetRawSize() const {
    ui64 result = 0;
    for (auto&& i : Array->chunks()) {
        result += NArrow::GetArrayDataSize(i);
    }
    return result;
}

std::shared_ptr<arrow::Scalar> TTrivialChunkedArray::DoGetMaxScalar() const {
    std::shared_ptr<arrow::Scalar> result;
    for (auto&& i : Array->chunks()) {
        if (!i->length()) {
            continue;
        }
        auto minMaxPos = NArrow::FindMinMaxPosition(i);
        auto scalarCurrent = NArrow::TStatusValidator::GetValid(i->GetScalar(minMaxPos.second));
        if (!result || ScalarCompare(result, scalarCurrent) < 0) {
            result = scalarCurrent;
        }
    }

    return result;
}

}   // namespace NKikimr::NArrow::NAccessor

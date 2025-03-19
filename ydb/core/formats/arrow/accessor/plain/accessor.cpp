#include "accessor.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/splitter/simple.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NAccessor {

std::optional<ui64> TTrivialArray::DoGetRawSize() const {
    return NArrow::GetArrayDataSize(Array);
}

std::shared_ptr<arrow::Scalar> TTrivialArray::DoGetMaxScalar() const {
    auto minMaxPos = NArrow::FindMinMaxPosition(Array);
    return NArrow::TStatusValidator::GetValid(Array->GetScalar(minMaxPos.second));
}

ui32 TTrivialArray::DoGetValueRawBytes() const {
    return NArrow::GetArrayDataSize(Array);
}

std::shared_ptr<TTrivialArray> TTrivialArray::BuildEmpty(const std::shared_ptr<arrow::DataType>& type) {
    return std::make_shared<TTrivialArray>(TThreadSimpleArraysCache::GetNull(type, 0));
}

void TTrivialArray::Reallocate() {
    Array = NArrow::ReallocateArray(Array);
}

std::shared_ptr<arrow::Array> TTrivialArray::BuildArrayFromOptionalScalar(
    const std::shared_ptr<arrow::Scalar>& scalar, const std::shared_ptr<arrow::DataType>& type) {
    if (scalar) {
        AFL_VERIFY(scalar->type->id() == type->id());
        auto builder = NArrow::MakeBuilder(scalar->type, 1);
        TStatusValidator::Validate(builder->AppendScalar(*scalar));
        return NArrow::FinishBuilder(std::move(builder));
    } else {
        auto builder = NArrow::MakeBuilder(type, 1);
        TStatusValidator::Validate(builder->AppendNull());
        return NArrow::FinishBuilder(std::move(builder));
    }
}

std::optional<bool> TTrivialArray::DoCheckOneValueAccessor(std::shared_ptr<arrow::Scalar>& value) const {
    if (Array->length() == 1) {
        value = TStatusValidator::GetValid(Array->GetScalar(0));
        return true;
    }
    return {};
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
        return (ui64)ChunkedArray->chunks()[idx]->length();
    }
    void OnArray(const ui32 idx, const ui32 startPosition) const {
        *Result = IChunkedArray::TLocalDataAddress(ChunkedArray->chunk(idx), startPosition, idx);
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

ui32 TTrivialChunkedArray::DoGetValueRawBytes() const {
    ui32 result = 0;
    for (auto&& i : Array->chunks()) {
        result += NArrow::GetArrayDataSize(i);
    }
    return result;
}

}   // namespace NKikimr::NArrow::NAccessor

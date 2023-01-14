#include "arrow_util.h"
#include "mkql_bit_utils.h"

#include <arrow/array/array_base.h>
#include <arrow/chunked_array.h>

#include <ydb/library/yql/minikql/mkql_node_builder.h>

#include <util/system/yassert.h>

namespace NKikimr::NMiniKQL {

std::shared_ptr<arrow::ArrayData> DeepSlice(const std::shared_ptr<arrow::ArrayData>& data, size_t offset, size_t len) {
    Y_VERIFY(data->length >= 0);
    Y_VERIFY(offset + len <= (size_t)data->length);
    if (offset == 0 && len == (size_t)data->length) {
        return data;
    }

    std::shared_ptr<arrow::ArrayData> result = data->Copy();
    result->offset = data->offset + offset;
    result->length = len;

    if (data->null_count == data->length) {
        result->null_count = len;
    } else if (len == 0) {
        result->null_count = 0;
    } else {
        result->null_count = data->null_count != 0 ? arrow::kUnknownNullCount : 0;
    }

    for (size_t i = 0; i < data->child_data.size(); ++i) {
        result->child_data[i] = DeepSlice(data->child_data[i], offset, len);
    }

    return result;
}

std::shared_ptr<arrow::ArrayData> Chop(std::shared_ptr<arrow::ArrayData>& data, size_t len) {
    auto first = DeepSlice(data, 0, len);
    data = DeepSlice(data, len, data->length - len);
    return first;
}

std::shared_ptr<arrow::ArrayData> Unwrap(const arrow::ArrayData& data, TType* itemType) {
    bool isOptional;
    auto unpacked = UnpackOptional(itemType, isOptional);
    MKQL_ENSURE(isOptional, "Expected optional");
    if (unpacked->IsOptional() || unpacked->IsVariant()) {
        MKQL_ENSURE(data.child_data.size() == 1, "Expected struct with one element");
        return data.child_data[0];
    } else {
        auto buffers = data.buffers;
        MKQL_ENSURE(buffers.size() >= 1, "Missing nullable bitmap");
        buffers[0] = nullptr;
        return arrow::ArrayData::Make(data.type, data.length, buffers, data.child_data, data.dictionary, 0, data.offset);
    }
}

std::shared_ptr<arrow::Buffer> AllocateBitmapWithReserve(size_t bitCount, arrow::MemoryPool* pool) {
    // align up to 64 bit
    bitCount = (bitCount + 63u) & ~size_t(63u);
    // this simplifies code compression code - we can write single 64 bit word after array boundaries
    bitCount += 64;
    return ARROW_RESULT(arrow::AllocateBitmap(bitCount, pool));
}

std::shared_ptr<arrow::Buffer> MakeDenseBitmap(const ui8* srcSparse, size_t len, arrow::MemoryPool* pool) {
    auto bitmap = AllocateBitmapWithReserve(len, pool);
    CompressSparseBitmap(bitmap->mutable_data(), srcSparse, len);
    return bitmap;
}

void ForEachArrayData(const arrow::Datum& datum, const std::function<void(const std::shared_ptr<arrow::ArrayData>&)>& func) {
    MKQL_ENSURE(datum.is_arraylike(), "Expected array");
    if (datum.is_array()) {
        func(datum.array());
    } else {
        for (auto& chunk : datum.chunks()) {
            func(chunk->data());
        }
    }
}

arrow::Datum MakeArray(const TVector<std::shared_ptr<arrow::ArrayData>>& chunks) {
    MKQL_ENSURE(!chunks.empty(), "Expected non empty chunks");
    arrow::ArrayVector resultChunks;
    for (auto& chunk : chunks) {
        resultChunks.push_back(arrow::Datum(chunk).make_array());
    }

    if (resultChunks.size() > 1) {
        auto type = resultChunks.front()->type();
        auto chunked = ARROW_RESULT(arrow::ChunkedArray::Make(std::move(resultChunks), type));
        return arrow::Datum(chunked);
    }
    return arrow::Datum(resultChunks.front());
}

}

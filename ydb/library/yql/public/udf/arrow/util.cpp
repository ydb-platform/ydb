#include "util.h"
#include "bit_util.h"
#include "defs.h"

#include <arrow/array/array_base.h>
#include <arrow/array/util.h>
#include <arrow/chunked_array.h>
#include <arrow/record_batch.h>

namespace NYql {
namespace NUdf {

namespace {

ui64 GetSizeOfArrayDataInBytes(const arrow::ArrayData& data) {
    ui64 size = sizeof(data);
    size += data.buffers.size() * sizeof(void*);
    size += data.child_data.size() * sizeof(void*);
    for (const auto& b : data.buffers) {
        if (b) {
            size += b->size();
        }
    }

    for (const auto& c : data.child_data) {
        if (c) {
            size += GetSizeOfArrayDataInBytes(*c);
        }
    }

    return size;
}

ui64 GetSizeOfDatumInBytes(const arrow::Datum& datum) {
    ui64 size = sizeof(datum);
    if (datum.is_scalar()) {
        const auto& scarray = ARROW_RESULT(arrow::MakeArrayFromScalar(*datum.scalar(), 1));
        return size + GetSizeOfArrayDataInBytes(*scarray->data());
    }
    if (datum.is_arraylike()) {
        ForEachArrayData(datum, [&size](const auto& arrayData) {
            size += GetSizeOfArrayDataInBytes(*arrayData);
        });
        return size;
    }
    Y_ABORT("Not yet implemented");
}

} // namespace

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

std::shared_ptr<arrow::Buffer> MakeDenseBitmapNegate(const ui8* srcSparse, size_t len, arrow::MemoryPool* pool) {
    auto bitmap = AllocateBitmapWithReserve(len, pool);
    CompressSparseBitmapNegate(bitmap->mutable_data(), srcSparse, len);
    return bitmap;
}

std::shared_ptr<arrow::ArrayData> DeepSlice(const std::shared_ptr<arrow::ArrayData>& data, size_t offset, size_t len) {
    Y_ENSURE(data->length >= 0);
    Y_ENSURE(offset + len <= (size_t)data->length);
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

std::shared_ptr<arrow::ArrayData> Unwrap(const arrow::ArrayData& data, bool isNestedOptional) {
    Y_ENSURE(data.GetNullCount() == 0);
    if (isNestedOptional) {
        Y_ENSURE(data.buffers.size() == 1);
        Y_ENSURE(data.child_data.size() == 1);
        return data.child_data.front();
    }
    auto result = data.Copy();
    result->buffers.front().reset();
    return result;
}

void ForEachArrayData(const arrow::Datum& datum, const std::function<void(const std::shared_ptr<arrow::ArrayData>&)>& func) {
    Y_ENSURE(datum.is_arraylike(), "Expected array");
    if (datum.is_array()) {
        func(datum.array());
    } else {
        for (auto& chunk : datum.chunks()) {
            func(chunk->data());
        }
    }
}

arrow::Datum MakeArray(const TVector<std::shared_ptr<arrow::ArrayData>>& chunks) {
    Y_ENSURE(!chunks.empty(), "Expected non empty chunks");
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

ui64 GetSizeOfArrowBatchInBytes(const arrow::RecordBatch& batch) {
    ui64 size = sizeof(batch);
    size += batch.num_columns() * sizeof(void*);
    for (int i = 0; i < batch.num_columns(); ++i) {
        size += GetSizeOfArrayDataInBytes(*batch.column_data(i));
    }

    return size;
}

ui64 GetSizeOfArrowExecBatchInBytes(const arrow::compute::ExecBatch& batch) {
    ui64 size = sizeof(batch);
    size += batch.num_values() * sizeof(void*);
    for (const auto& datum : batch.values) {
        size += GetSizeOfDatumInBytes(datum);
    }

    return size;
}
}
}

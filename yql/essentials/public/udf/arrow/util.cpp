#include "util.h"
#include "bit_util.h"
#include "defs.h"

#include <arrow/array/array_base.h>
#include <arrow/array/util.h>
#include <arrow/chunked_array.h>
#include <arrow/record_batch.h>
#include <arrow/util/bitmap_ops.h>

namespace NYql::NUdf {

namespace {

ui64 GetSizeOfDatumInBytes(const arrow20::Datum& datum) {
    ui64 size = sizeof(datum);
    if (datum.is_scalar()) {
        const auto& scarray = ARROW_RESULT(arrow20::MakeArrayFromScalar(*datum.scalar(), 1));
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

std::shared_ptr<arrow20::Buffer> AllocateBitmapWithReserve(size_t bitCount, arrow20::MemoryPool* pool) {
    // align up to 64 bit
    bitCount = (bitCount + 63u) & ~size_t(63u);
    // this simplifies code compression code - we can write single 64 bit word after array boundaries
    bitCount += 64;
    return ARROW_RESULT(arrow20::AllocateBitmap(bitCount, pool));
}

std::shared_ptr<arrow20::Buffer> MakeDenseBitmap(const ui8* srcSparse, size_t len, arrow20::MemoryPool* pool) {
    auto bitmap = AllocateBitmapWithReserve(len, pool);
    CompressSparseBitmap(bitmap->mutable_data(), srcSparse, len);
    return bitmap;
}

std::shared_ptr<arrow20::Buffer> MakeDenseBitmapNegate(const ui8* srcSparse, size_t len, arrow20::MemoryPool* pool) {
    auto bitmap = AllocateBitmapWithReserve(len, pool);
    CompressSparseBitmapNegate(bitmap->mutable_data(), srcSparse, len);
    return bitmap;
}

std::shared_ptr<arrow20::Buffer> MakeDenseBitmapCopy(const ui8* src, size_t len, size_t offset, arrow20::MemoryPool* pool) {
    auto bitmap = AllocateBitmapWithReserve(len, pool);
    CopyDenseBitmap(bitmap->mutable_data(), src, offset, len);
    return bitmap;
}

std::shared_ptr<arrow20::Buffer> MakeDenseBitmapCopyIfOffsetDiffers(std::shared_ptr<arrow20::Buffer> src, size_t len, size_t sourceOffset, size_t resultOffset, arrow20::MemoryPool* pool) {
    if ((sourceOffset == resultOffset) || !src) {
        return src;
    }
    auto bitmap = AllocateBitmapWithReserve(len + resultOffset, pool);
    arrow20::internal::CopyBitmap(src->data(), sourceOffset, len, bitmap->mutable_data(), resultOffset);
    return bitmap;
}

std::shared_ptr<arrow20::Buffer> MakeDenseFalseBitmap(int64_t len, arrow20::MemoryPool* pool) {
    auto bitmap = AllocateBitmapWithReserve(len, pool);
    std::memset(bitmap->mutable_data(), 0, bitmap->size());
    return bitmap;
}

std::shared_ptr<arrow20::ArrayData> DeepSlice(const std::shared_ptr<arrow20::ArrayData>& data, size_t offset, size_t len) {
    Y_ENSURE(data->length >= 0);
    Y_ENSURE(offset + len <= (size_t)data->length);
    if (offset == 0 && len == (size_t)data->length) {
        return data;
    }

    std::shared_ptr<arrow20::ArrayData> result = data->Copy();
    result->offset = data->offset + offset;
    result->length = len;

    if (data->null_count == data->length) {
        result->null_count = len;
    } else if (len == 0) {
        result->null_count = 0;
    } else {
        result->null_count = data->null_count != 0 ? arrow20::kUnknownNullCount : 0;
    }

    for (size_t i = 0; i < data->child_data.size(); ++i) {
        result->child_data[i] = DeepSlice(data->child_data[i], offset, len);
    }

    return result;
}

std::shared_ptr<arrow20::ArrayData> Chop(std::shared_ptr<arrow20::ArrayData>& data, size_t len) {
    auto first = DeepSlice(data, 0, len);
    data = DeepSlice(data, len, data->length - len);
    return first;
}

void ForEachArrayData(const arrow20::Datum& datum, const std::function<void(const std::shared_ptr<arrow20::ArrayData>&)>& func) {
    Y_ENSURE(datum.is_arraylike(), "Expected array");
    if (datum.is_array()) {
        func(datum.array());
    } else {
        for (auto& chunk : datum.chunks()) {
            func(chunk->data());
        }
    }
}

arrow20::Datum MakeArray(const TVector<std::shared_ptr<arrow20::ArrayData>>& chunks) {
    Y_ENSURE(!chunks.empty(), "Expected non empty chunks");
    arrow20::ArrayVector resultChunks;
    for (auto& chunk : chunks) {
        resultChunks.push_back(arrow20::Datum(chunk).make_array());
    }

    if (resultChunks.size() > 1) {
        auto type = resultChunks.front()->type();
        auto chunked = ARROW_RESULT(arrow20::ChunkedArray::Make(std::move(resultChunks), type));
        return arrow20::Datum(chunked);
    }
    return arrow20::Datum(resultChunks.front());
}

ui64 GetSizeOfArrayDataInBytes(const arrow20::ArrayData& data) {
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

ui64 GetSizeOfArrowBatchInBytes(const arrow20::RecordBatch& batch) {
    ui64 size = sizeof(batch);
    size += batch.num_columns() * sizeof(void*);
    for (int i = 0; i < batch.num_columns(); ++i) {
        size += GetSizeOfArrayDataInBytes(*batch.column_data(i));
    }

    return size;
}

ui64 GetSizeOfArrowExecBatchInBytes(const arrow20::compute::ExecBatch& batch) {
    ui64 size = sizeof(batch);
    size += batch.num_values() * sizeof(void*);
    for (const auto& datum : batch.values) {
        size += GetSizeOfDatumInBytes(datum);
    }

    return size;
}

const TType* SkipTaggedType(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    TTaggedTypeInspector typeTagged(typeInfoHelper, type);
    while (typeTagged) {
        type = typeTagged.GetBaseType();
        typeTagged = TTaggedTypeInspector(typeInfoHelper, type);
    }

    return type;
}
} // namespace NYql::NUdf

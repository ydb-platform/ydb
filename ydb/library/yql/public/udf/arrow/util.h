#pragma once

#include "defs.h"

#include <util/generic/vector.h>

#include <arrow/buffer_builder.h>
#include <arrow/datum.h>
#include <arrow/util/bit_util.h>

#include <functional>

namespace NYql {
namespace NUdf {

std::shared_ptr<arrow::Buffer> AllocateBitmapWithReserve(size_t bitCount, arrow::MemoryPool* pool);
std::shared_ptr<arrow::Buffer> MakeDenseBitmap(const ui8* srcSparse, size_t len, arrow::MemoryPool* pool);

/// \brief Recursive version of ArrayData::Slice() method
std::shared_ptr<arrow::ArrayData> DeepSlice(const std::shared_ptr<arrow::ArrayData>& data, size_t offset, size_t len);

/// \brief Chops first len items of `data` as new ArrayData object
std::shared_ptr<arrow::ArrayData> Chop(std::shared_ptr<arrow::ArrayData>& data, size_t len);

void ForEachArrayData(const arrow::Datum& datum, const std::function<void(const std::shared_ptr<arrow::ArrayData>&)>& func);
arrow::Datum MakeArray(const TVector<std::shared_ptr<arrow::ArrayData>>& chunks);

inline bool IsNull(const arrow::ArrayData& data, size_t index) {
    return data.GetNullCount() > 0 && !arrow::BitUtil::GetBit(data.GetValues<uint8_t>(0, 0), index + data.offset);
}

// similar to arrow::TypedBufferBuilder, but with UnsafeAdvance() method
// and shrinkToFit = false
template<typename T>
class TTypedBufferBuilder {
    static_assert(std::is_pod_v<T>);
    static_assert(!std::is_same_v<T, bool>);
public:
    explicit TTypedBufferBuilder(arrow::MemoryPool* pool)
        : Builder(pool)
    {
    }

    inline void Reserve(size_t size) {
        ARROW_OK(Builder.Reserve(size * sizeof(T)));
    }

    inline size_t Length() const {
        return Builder.length() / sizeof(T);
    }

    inline T* MutableData() {
        return reinterpret_cast<T*>(Builder.mutable_data());
    }

    inline T* End() {
        return MutableData() + Length();
    }

    inline const T* Data() const {
        return reinterpret_cast<const T*>(Builder.data());
    }

    inline void UnsafeAppend(const T* values, size_t count) {
        std::memcpy(End(), values, count * sizeof(T));
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(size_t count, const T& value) {
        T* target = End();
        std::fill(target, target + count, value);
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(T&& value) {
        *End() = std::move(value);
        UnsafeAdvance(1);
    }

    inline void UnsafeAdvance(size_t count) {
        Builder.UnsafeAdvance(count * sizeof(T));
    }

    inline std::shared_ptr<arrow::Buffer> Finish() {
        bool shrinkToFit = false;
        return ARROW_RESULT(Builder.Finish(shrinkToFit));
    }
private:
    arrow::BufferBuilder Builder;
};

}
}

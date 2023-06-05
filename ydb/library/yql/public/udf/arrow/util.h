#pragma once

#include "defs.h"

#include <util/generic/vector.h>

#include <arrow/datum.h>
#include <arrow/memory_pool.h>
#include <arrow/util/bit_util.h>

#include <functional>

namespace NYql {
namespace NUdf {

enum class EPgStringType {
    None,
    Text,
    CString
};

std::shared_ptr<arrow::Buffer> AllocateBitmapWithReserve(size_t bitCount, arrow::MemoryPool* pool);
std::shared_ptr<arrow::Buffer> MakeDenseBitmap(const ui8* srcSparse, size_t len, arrow::MemoryPool* pool);
std::shared_ptr<arrow::Buffer> MakeDenseBitmapNegate(const ui8* srcSparse, size_t len, arrow::MemoryPool* pool);

/// \brief Recursive version of ArrayData::Slice() method
std::shared_ptr<arrow::ArrayData> DeepSlice(const std::shared_ptr<arrow::ArrayData>& data, size_t offset, size_t len);

/// \brief Chops first len items of `data` as new ArrayData object
std::shared_ptr<arrow::ArrayData> Chop(std::shared_ptr<arrow::ArrayData>& data, size_t len);

/// \brief Unwrap array (decrease optional level)
std::shared_ptr<arrow::ArrayData> Unwrap(const arrow::ArrayData& data, bool isNestedOptional);

void ForEachArrayData(const arrow::Datum& datum, const std::function<void(const std::shared_ptr<arrow::ArrayData>&)>& func);
arrow::Datum MakeArray(const TVector<std::shared_ptr<arrow::ArrayData>>& chunks);

inline bool IsNull(const arrow::ArrayData& data, size_t index) {
    return data.GetNullCount() > 0 && !arrow::BitUtil::GetBit(data.GetValues<uint8_t>(0, 0), index + data.offset);
}

/// \brief same as arrow::AllocateResizableBuffer, but allows to control zero padding
std::unique_ptr<arrow::ResizableBuffer> AllocateResizableBuffer(size_t size, arrow::MemoryPool* pool, bool zeroPad = false);

ui64 GetSizeOfArrowBatchInBytes(const arrow::RecordBatch& batch);

// similar to arrow::TypedBufferBuilder, but:
// 1) with UnsafeAdvance() method
// 2) shrinkToFit = false
// 3) doesn't zero pad buffer
template<typename T>
class TTypedBufferBuilder {
    static_assert(std::is_pod_v<T>);
    static_assert(!std::is_same_v<T, bool>);
public:
    explicit TTypedBufferBuilder(arrow::MemoryPool* pool)
        : Pool(pool)
    {
    }

    inline void Reserve(size_t size) {
        if (!Buffer) {
            bool zeroPad = false;
            Buffer = AllocateResizableBuffer(size * sizeof(T), Pool, zeroPad);
        } else {
            size_t requiredBytes = (size + Length()) * sizeof(T);
            size_t currentCapacity = Buffer->capacity();
            if (requiredBytes > currentCapacity) {
                size_t newCapacity = std::max(requiredBytes, currentCapacity * 2);
                ARROW_OK(Buffer->Reserve(newCapacity));
            }
        }
    }

    inline size_t Length() const {
        return Len;
    }

    inline T* MutableData() {
        return reinterpret_cast<T*>(Buffer->mutable_data());
    }

    inline T* End() {
        return MutableData() + Length();
    }

    inline const T* Data() const {
        return reinterpret_cast<const T*>(Buffer->data());
    }

    inline void UnsafeAppend(const T* values, size_t count) {
        Y_VERIFY_DEBUG(count + Length() <= Buffer->capacity() / sizeof(T));
        std::memcpy(End(), values, count * sizeof(T));
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(size_t count, const T& value) {
        Y_VERIFY_DEBUG(count + Length() <= Buffer->capacity() / sizeof(T));
        T* target = End();
        std::fill(target, target + count, value);
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(T&& value) {
        Y_VERIFY_DEBUG(1 + Length() <= Buffer->capacity() / sizeof(T));
        *End() = std::move(value);
        UnsafeAdvance(1);
    }

    inline void UnsafeAdvance(size_t count) {
        Len += count;
    }

    inline std::shared_ptr<arrow::Buffer> Finish() {
        bool shrinkToFit = false;
        ARROW_OK(Buffer->Resize(Len * sizeof(T), shrinkToFit));
        std::shared_ptr<arrow::ResizableBuffer> result;
        std::swap(result, Buffer);
        Len = 0;
        return result;
    }
private:
    arrow::MemoryPool* const Pool;
    std::shared_ptr<arrow::ResizableBuffer> Buffer;
    size_t Len = 0;
};

inline void* GetMemoryContext(const void* ptr) {
    return *(void**)((char*)ptr - sizeof(void*));
}

inline void SetMemoryContext(void* ptr, void* ctx) {
    *(void**)((char*)ptr - sizeof(void*)) = ctx;
}

inline void ZeroMemoryContext(void* ptr) {
    SetMemoryContext(ptr, nullptr);
}

}
}

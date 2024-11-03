#pragma once

#include "defs.h"

#include <util/generic/vector.h>

#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/memory_pool.h>
#include <arrow/util/bit_util.h>

#include <functional>

namespace NYql {
namespace NUdf {

enum class EPgStringType {
    None,
    Text,
    CString,
    Fixed
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

ui64 GetSizeOfArrowBatchInBytes(const arrow::RecordBatch& batch);
ui64 GetSizeOfArrowExecBatchInBytes(const arrow::compute::ExecBatch& batch);

class TResizeableBuffer : public arrow::ResizableBuffer {
public:
    explicit TResizeableBuffer(arrow::MemoryPool* pool)
        : ResizableBuffer(nullptr, 0, arrow::CPUDevice::memory_manager(pool))
        , Pool(pool)
    {
    }
    ~TResizeableBuffer() override {
        uint8_t* ptr = mutable_data();
        if (ptr) {
            Pool->Free(ptr, capacity_);
        }
    }

    arrow::Status Reserve(const int64_t capacity) override {
        if (capacity < 0) {
            return arrow::Status::Invalid("Negative buffer capacity: ", capacity);
        }
        uint8_t* ptr = mutable_data();
        if (!ptr || capacity > capacity_) {
            int64_t newCapacity = arrow::BitUtil::RoundUpToMultipleOf64(capacity);
            if (ptr) {
                ARROW_RETURN_NOT_OK(Pool->Reallocate(capacity_, newCapacity, &ptr));
            } else {
                ARROW_RETURN_NOT_OK(Pool->Allocate(newCapacity, &ptr));
            }
            data_ = ptr;
            capacity_ = newCapacity;
        }
        return arrow::Status::OK();
    }

    arrow::Status Resize(const int64_t newSize, bool shrink_to_fit = true) override {
        if (ARROW_PREDICT_FALSE(newSize < 0)) {
            return arrow::Status::Invalid("Negative buffer resize: ", newSize);
        }
        uint8_t* ptr = mutable_data();
        if (ptr && shrink_to_fit && newSize <= size_) {
            int64_t newCapacity = arrow::BitUtil::RoundUpToMultipleOf64(newSize);
            if (capacity_ != newCapacity) {
                ARROW_RETURN_NOT_OK(Pool->Reallocate(capacity_, newCapacity, &ptr));
                data_ = ptr;
                capacity_ = newCapacity;
            }
        } else {
            RETURN_NOT_OK(Reserve(newSize));
        }
        size_ = newSize;

        return arrow::Status::OK();
    }

private:
    arrow::MemoryPool* Pool;
};

/// \brief same as arrow::AllocateResizableBuffer, but allows to control zero padding
template<typename TBuffer = TResizeableBuffer>
std::unique_ptr<arrow::ResizableBuffer> AllocateResizableBuffer(size_t size, arrow::MemoryPool* pool, bool zeroPad = false) {
    std::unique_ptr<TBuffer> result = std::make_unique<TBuffer>(pool);
    ARROW_OK(result->Reserve(size));
    if (zeroPad) {
        result->ZeroPadding();
    }
    return result;
}

/// \brief owning buffer that calls destructors
template<typename T>
class TResizableManagedBuffer final : public TResizeableBuffer {
    static_assert(!std::is_trivially_destructible_v<T>);
public:
    explicit TResizableManagedBuffer(arrow::MemoryPool* pool)
        : TResizeableBuffer(pool) {}

    ~TResizableManagedBuffer() override {
        for (int64_t i = 0; i < size_; i += sizeof(T)) {
            auto* ptr = reinterpret_cast<T*>(mutable_data() + i);
            ptr->~T();
        }
    }
};

// similar to arrow::TypedBufferBuilder, but:
// 1) with UnsafeAdvance() method
// 2) shrinkToFit = false
// 3) doesn't zero pad buffer
template<typename T>
class TTypedBufferBuilder {
    static_assert(!std::is_same_v<T, bool>);

    using TArrowBuffer = std::conditional_t<std::is_trivially_destructible_v<T>, TResizeableBuffer, TResizableManagedBuffer<T>>;
public:
    explicit TTypedBufferBuilder(arrow::MemoryPool* pool)
        : Pool(pool)
    {
    }

    inline void Reserve(size_t size) {
        if (!Buffer) {
            bool zeroPad = false;
            Buffer = AllocateResizableBuffer<TArrowBuffer>(size * sizeof(T), Pool, zeroPad);
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

    inline size_t Capacity() const {
        return Buffer ? size_t(Buffer->capacity()) : 0;
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
        Y_DEBUG_ABORT_UNLESS(count + Length() <= Buffer->capacity() / sizeof(T));
        std::memcpy(End(), values, count * sizeof(T));
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(size_t count, const T& value) {
        Y_DEBUG_ABORT_UNLESS(count + Length() <= Buffer->capacity() / sizeof(T));
        T* target = End();
        std::fill(target, target + count, value);
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(T&& value) {
        Y_DEBUG_ABORT_UNLESS(1 + Length() <= Buffer->capacity() / sizeof(T));
        *End() = std::move(value);
        UnsafeAdvance(1);
    }

    inline void UnsafeAdvance(size_t count) {
        Y_DEBUG_ABORT_UNLESS(count + Length() <= Buffer->capacity() / sizeof(T));
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

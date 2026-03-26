#pragma once

#include "defs.h"

#include <arrow/array/array_base.h>
#include <arrow/array/array_nested.h>
#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/memory_pool.h>
#include <arrow/util/bit_util.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <functional>

#include <yql/essentials/public/udf/udf_type_inspection.h>
#include <yql/essentials/public/udf/udf_types.h>

namespace NYql::NUdf {

enum class EPgStringType {
    None,
    Text,
    CString,
    Fixed
};

std::shared_ptr<arrow20::Buffer> AllocateBitmapWithReserve(size_t bitCount, arrow20::MemoryPool* pool);
std::shared_ptr<arrow20::Buffer> MakeDenseBitmap(const ui8* srcSparse, size_t len, arrow20::MemoryPool* pool);
std::shared_ptr<arrow20::Buffer> MakeDenseBitmapNegate(const ui8* srcSparse, size_t len, arrow20::MemoryPool* pool);
std::shared_ptr<arrow20::Buffer> MakeDenseBitmapCopy(const ui8* src, size_t len, size_t offset, arrow20::MemoryPool* pool);

// Note: return src if offsets are same so don't change result bitmap.
std::shared_ptr<arrow20::Buffer> MakeDenseBitmapCopyIfOffsetDiffers(std::shared_ptr<arrow20::Buffer> src, size_t len, size_t sourceOffset, size_t resultOffset, arrow20::MemoryPool* pool);

std::shared_ptr<arrow20::Buffer> MakeDenseFalseBitmap(int64_t len, arrow20::MemoryPool* pool);

/// \brief Recursive version of ArrayData::Slice() method
std::shared_ptr<arrow20::ArrayData> DeepSlice(const std::shared_ptr<arrow20::ArrayData>& data, size_t offset, size_t len);

/// \brief Chops first len items of `data` as new ArrayData object
std::shared_ptr<arrow20::ArrayData> Chop(std::shared_ptr<arrow20::ArrayData>& data, size_t len);

void ForEachArrayData(const arrow20::Datum& datum, const std::function<void(const std::shared_ptr<arrow20::ArrayData>&)>& func);
arrow20::Datum MakeArray(const TVector<std::shared_ptr<arrow20::ArrayData>>& chunks);

inline bool IsNull(const arrow20::ArrayData& data, size_t index) {
    return data.GetNullCount() > 0 && !arrow20::BitUtil::GetBit(data.GetValues<uint8_t>(0, 0), index + data.offset);
}

ui64 GetSizeOfArrayDataInBytes(const arrow20::ArrayData& data);
ui64 GetSizeOfArrowBatchInBytes(const arrow20::RecordBatch& batch);
ui64 GetSizeOfArrowExecBatchInBytes(const arrow20::compute::ExecBatch& batch);

class TResizeableBuffer: public arrow20::ResizableBuffer {
public:
    explicit TResizeableBuffer(arrow20::MemoryPool* pool)
        : ResizableBuffer(nullptr, 0, arrow20::CPUDevice::memory_manager(pool))
        , Pool_(pool)
    {
    }
    ~TResizeableBuffer() override {
        uint8_t* ptr = mutable_data();
        if (ptr) {
            Pool_->Free(ptr, capacity_);
        }
    }

    arrow20::Status Reserve(const int64_t capacity) override {
        if (capacity < 0) {
            return arrow20::Status::Invalid("Negative buffer capacity: ", capacity);
        }
        uint8_t* ptr = mutable_data();
        if (!ptr || capacity > capacity_) {
            int64_t newCapacity = arrow20::BitUtil::RoundUpToMultipleOf64(capacity);
            if (ptr) {
                ARROW_RETURN_NOT_OK(Pool_->Reallocate(capacity_, newCapacity, &ptr));
            } else {
                ARROW_RETURN_NOT_OK(Pool_->Allocate(newCapacity, &ptr));
            }
            data_ = ptr;
            capacity_ = newCapacity;
        }
        return arrow20::Status::OK();
    }

    arrow20::Status Resize(const int64_t newSize, bool shrink_to_fit = true) override {
        if (ARROW_PREDICT_FALSE(newSize < 0)) {
            return arrow20::Status::Invalid("Negative buffer resize: ", newSize);
        }
        uint8_t* ptr = mutable_data();
        if (ptr && shrink_to_fit) {
            int64_t newCapacity = arrow20::BitUtil::RoundUpToMultipleOf64(newSize);
            if (capacity_ != newCapacity) {
                ARROW_RETURN_NOT_OK(Pool_->Reallocate(capacity_, newCapacity, &ptr));
                data_ = ptr;
                capacity_ = newCapacity;
            }
        } else {
            RETURN_NOT_OK(Reserve(newSize));
        }
        size_ = newSize;

        return arrow20::Status::OK();
    }

private:
    arrow20::MemoryPool* Pool_;
};

template <typename TBuffer = TResizeableBuffer>
std::unique_ptr<arrow20::ResizableBuffer> AllocateResizableBuffer(size_t capacity, arrow20::MemoryPool* pool, bool zeroPad = false) {
    std::unique_ptr<TBuffer> result = std::make_unique<TBuffer>(pool);
    ARROW_OK(result->Reserve(capacity));
    if (zeroPad) {
        result->ZeroPadding();
    }
    return result;
}

/// \brief owning buffer that calls destructors
template <typename T>
class TResizableManagedBuffer final: public TResizeableBuffer {
    static_assert(!std::is_trivially_destructible_v<T>);

public:
    explicit TResizableManagedBuffer(arrow20::MemoryPool* pool)
        : TResizeableBuffer(pool)
    {
    }

    ~TResizableManagedBuffer() override {
        for (int64_t i = 0; i < size_; i += sizeof(T)) {
            auto* ptr = reinterpret_cast<T*>(mutable_data() + i);
            ptr->~T();
        }
    }
};

// similar to arrow20::TypedBufferBuilder, but:
// 1) with UnsafeAdvance() method
// 2) shrinkToFit = false
// 3) doesn't zero pad buffer
template <typename T>
class TTypedBufferBuilder {
    static_assert(!std::is_same_v<T, bool>);

    using TArrowBuffer = std::conditional_t<std::is_trivially_destructible_v<T>, TResizeableBuffer, TResizableManagedBuffer<T>>;

public:
    explicit TTypedBufferBuilder(arrow20::MemoryPool* pool, TMaybe<ui8> minFillPercentage = {})
        : MinFillPercentage_(minFillPercentage)
        , Pool_(pool)
    {
        Y_ENSURE(!MinFillPercentage_ || *MinFillPercentage_ <= 100);
    }

    inline void Reserve(size_t size) {
        if (!Buffer_) {
            bool zeroPad = false;
            Buffer_ = AllocateResizableBuffer<TArrowBuffer>(size * sizeof(T), Pool_, zeroPad);
        } else {
            size_t requiredBytes = (size + Length()) * sizeof(T);
            size_t currentCapacity = Buffer_->capacity();
            if (requiredBytes > currentCapacity) {
                size_t newCapacity = std::max(requiredBytes, currentCapacity * 2);
                ARROW_OK(Buffer_->Reserve(newCapacity));
            }
        }
    }

    inline size_t Length() const {
        return Len_;
    }

    inline size_t Capacity() const {
        return Buffer_ ? size_t(Buffer_->capacity()) : 0;
    }

    inline T* MutableData() {
        return reinterpret_cast<T*>(Buffer_->mutable_data());
    }

    inline T* End() {
        return MutableData() + Length();
    }

    inline const T* Data() const {
        return reinterpret_cast<const T*>(Buffer_->data());
    }

    inline void UnsafeAppend(const T* values, size_t count) {
        Y_DEBUG_ABORT_UNLESS(count + Length() <= Buffer_->capacity() / sizeof(T));
        std::memcpy(End(), values, count * sizeof(T));
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(size_t count, const T& value) {
        Y_DEBUG_ABORT_UNLESS(count + Length() <= Buffer_->capacity() / sizeof(T));
        T* target = End();
        std::fill(target, target + count, value);
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(T&& value) {
        Y_DEBUG_ABORT_UNLESS(1 + Length() <= Buffer_->capacity() / sizeof(T));
        *End() = std::move(value);
        UnsafeAdvance(1);
    }

    inline void UnsafeAdvance(size_t count) {
        Y_DEBUG_ABORT_UNLESS(count + Length() <= Buffer_->capacity() / sizeof(T));
        Len_ += count;
    }

    inline std::shared_ptr<arrow20::Buffer> Finish() {
        int64_t newSize = Len_ * sizeof(T);
        bool shrinkToFit = MinFillPercentage_
                               ? newSize <= Buffer_->capacity() * *MinFillPercentage_ / 100
                               : false;
        ARROW_OK(Buffer_->Resize(newSize, shrinkToFit));
        std::shared_ptr<arrow20::ResizableBuffer> result;
        std::swap(result, Buffer_);
        Len_ = 0;
        return result;
    }

private:
    const TMaybe<ui8> MinFillPercentage_;
    arrow20::MemoryPool* const Pool_;
    std::shared_ptr<arrow20::ResizableBuffer> Buffer_;
    size_t Len_ = 0;
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

inline bool IsSingularType(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    auto kind = typeInfoHelper.GetTypeKind(type);
    return kind == ETypeKind::Null ||
           kind == ETypeKind::Void ||
           kind == ETypeKind::EmptyDict ||
           kind == ETypeKind::EmptyList;
}

const TType* SkipTaggedType(const ITypeInfoHelper& typeInfoHelper, const TType* type);

inline bool NeedWrapWithExternalOptional(const ITypeInfoHelper& typeInfoHelper, const TType* type) {
    type = SkipTaggedType(typeInfoHelper, type);
    TOptionalTypeInspector typeOpt(typeInfoHelper, type);
    if (!typeOpt) {
        return false;
    }
    type = SkipTaggedType(typeInfoHelper, typeOpt.GetItemType());
    TOptionalTypeInspector typeOptOpt(typeInfoHelper, type);
    if (typeOptOpt) {
        return true;
    } else if (TPgTypeInspector(typeInfoHelper, type) || IsSingularType(typeInfoHelper, type)) {
        return true;
    }
    return false;
}

inline std::shared_ptr<arrow20::DataType> MakeSingularType(bool isNull) {
    if (isNull) {
        return arrow20::null();
    } else {
        return std::make_shared<arrow20::StructType>(std::vector<std::shared_ptr<arrow20::Field>>{});
    }
}

inline std::shared_ptr<arrow20::ArrayData> MakeSingularArray(bool isNull, i64 length) {
    if (isNull) {
        return arrow20::NullArray(length).data();
    } else {
        return arrow20::StructArray(MakeSingularType(/*isNull=*/false), length, /*children=*/{}, nullptr, /*null_count=*/0).data();
    }
}

inline std::shared_ptr<arrow20::Scalar> MakeSingularScalar(bool isNull) {
    if (isNull) {
        return arrow20::MakeNullScalar(MakeSingularType(/*isNull=*/true));
    } else {
        return std::make_shared<arrow20::StructScalar>(std::vector<std::shared_ptr<arrow20::Scalar>>{}, MakeSingularType(/*isNull=*/false));
    }
}

} // namespace NYql::NUdf

#pragma once

#include "udf_types.h"
#include "udf_allocator.h"
#include "udf_string_ref.h"
#include "udf_type_size_check.h"

#include <util/generic/strbuf.h>
#include <util/system/align.h>

#include <new>

namespace NYql {
namespace NUdf {

//////////////////////////////////////////////////////////////////////////////
// TStringValue
//////////////////////////////////////////////////////////////////////////////
class TStringValue
{
    friend class TUnboxedValue;
    friend class TUnboxedValuePod;
    friend class TBlockItem;
    friend struct TRawStringValue;

    class TData {
        friend class TStringValue;
    public:
        inline TData(ui32 size, ui64 cap)
            : Size_(size)
            , Refs_(1)
            , Capacity_(cap)
        {}

        inline i32 RefCount() const { return Refs_; }
        inline void Ref() {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
            if (Refs_ < 0)
                return;
#endif
            Refs_++;
        }
        inline void UnRef() {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
            if (Refs_ < 0)
                return;
#endif
            Y_DEBUG_ABORT_UNLESS(Refs_ > 0);
            if (!--Refs_) {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
                UdfFreeWithSize(this, sizeof(*this) + Capacity_);
#else
                UdfFree(this);
#endif
            }
        }
        inline void ReleaseRef() {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
            if (Refs_ < 0)
                return;
#endif
            Y_DEBUG_ABORT_UNLESS(Refs_ > 0);
            --Refs_;
        }
        inline void DeleteUnreferenced() {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
            if (Refs_ < 0)
                return;
#endif
            if (!Refs_) {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
                UdfFreeWithSize(this, sizeof(*this) + Capacity_);
#else
                UdfFree(this);
#endif
            }
        }

        inline char* Data() const { return (char*)(this+1); }
        inline ui32 Size() const { return Size_; }
        inline bool Empty() const { return Size_ == 0; }
        inline ui64 Capacity() const { return Capacity_; }

        inline i32 LockRef() noexcept {
            Y_DEBUG_ABORT_UNLESS(Refs_ != -1);
            auto ret = Refs_;
            Refs_ = -1;
            return ret;
        }

        inline void UnlockRef(i32 prev) noexcept {
            Y_DEBUG_ABORT_UNLESS(Refs_ == -1);
            Y_DEBUG_ABORT_UNLESS(prev != -1);
            Refs_ = prev;
        }

    private:
        ui32 Size_;
        i32  Refs_;
        ui64 Capacity_;
    };

    UDF_ASSERT_TYPE_SIZE(TData, 16);

public:
    inline explicit TStringValue(ui32 size): Data_(AllocateData(size, size)) {}
    inline explicit TStringValue(TData* data): Data_(data) { Ref(); }
    inline explicit TStringValue(TStringRef data): Data_(AllocateData(data.Size(), data.Size())) {
        memcpy(Data_->Data(), data.Data(), data.Size());
    }

    inline TStringValue(const TStringValue& rhs): Data_(rhs.Data_) { Ref(); }
    inline TStringValue(TStringValue&& rhs): Data_(rhs.Data_) { rhs.Data_ = nullptr; }

    inline ~TStringValue() { UnRef(); }

    inline TStringValue& operator=(const TStringValue& rhs) {
        if (this != &rhs) {
            UnRef();
            Data_ = rhs.Data_;
            Ref();
        }

        return *this;
    }

    TStringValue& operator=(TStringValue&& rhs) {
        if (this != &rhs) {
            UnRef();
            Data_ = rhs.Data_;
            rhs.Data_ = nullptr;
        }

        return *this;
    }

    inline ui64 Capacity() const { return Data_->Capacity(); }
    inline ui32 Size() const { return Data_->Size(); }
    inline char* Data() const { return Data_->Data(); }

    inline void Ref() {
        if (Data_ != nullptr) {
            Data_->Ref();
        }
    }

    inline void UnRef() {
        if (Data_ != nullptr) {
            Data_->UnRef();
            Data_ = nullptr;
        }
    }

    inline void ReleaseRef() {
        if (Data_ != nullptr) {
            Data_->ReleaseRef();
            Data_ = nullptr;
        }
    }

    inline i32 RefCount() const {
        return Data_->RefCount();
    }

    bool TryExpandOn(ui32 len) {
        if (RefCount() < 0)
           return false;

        const auto size = Data_->Size_ + len;
        if (Data_->Capacity_ < size)
            return false;

        Data_->Size_ = size;
        return true;
    }

    inline i32 LockRef() noexcept {
        return Data_->LockRef();
    }

    inline void UnlockRef(i32 prev) noexcept {
        Data_->UnlockRef(prev);
    }

private:
    inline TData* GetBuf() const {
        return Data_;
    }

    inline TData* ReleaseBuf() {
        if (const auto data = Data_) {
            Data_ = nullptr;
            data->ReleaseRef();
            return data;
        }
        return nullptr;
    }

public:
    static TData* AllocateData(ui32 len, ui32 cap) {
        const auto alligned = AlignUp<ui64>(cap, 16ULL);
        const auto dataSize = sizeof(TData) + alligned;
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
        return ::new(UdfAllocateWithSize(dataSize)) TData(len, alligned);
#else
        return ::new(UdfAllocate(dataSize)) TData(len, alligned);
#endif
    }

    TData* Data_;
};

UDF_ASSERT_TYPE_SIZE(TStringValue, 8);

} // namspace NUdf
} // namspace NYql

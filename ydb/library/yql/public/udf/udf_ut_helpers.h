#pragma once
#include "udf_value.h"

#include <util/generic/ymath.h>
#include <util/system/platform.h>
#include <util/generic/yexception.h>
#include <util/system/yassert.h>
#include <util/string/hex.h>

namespace NYql {
namespace NUdf {

inline size_t GetMethodPtrIndex(uintptr_t ptr)
{
#ifdef _win_
    size_t offset;
    if (memcmp((void*)ptr, "\x48\x8B\x01\xFF", 4) == 0) {
        if (*(ui8*)(ptr + 4) == 0x60) {
            offset = *(ui8*)(ptr + 5);
        } else if (*(ui8*)(ptr + 4) == 0xa0) {
            offset = *(ui32*)(ptr + 5);
        } else {
            ythrow yexception() << "Unsupported code:" << HexEncode((char*)ptr + 4, 1);
        }
    } else if (memcmp((void*)ptr, "\x50\x48\x89\x0c\x24\x48\x8b\x0c\x24\x48\x8b\x01\x48\x8b", 14) == 0) {
        if (*(ui8*)(ptr + 14) == 0x40) {
            offset = *(ui8*)(ptr + 15);
        } else if (*(ui8*)(ptr + 14) == 0x80) {
            offset = *(ui32*)(ptr + 15);
        } else {
            ythrow yexception() << "Unsupported code:" << HexEncode((char*)ptr + 14, 1);
        }
    } else {
        ythrow yexception() << "Unsupported code: " << HexEncode((char*)ptr, 16);
    }

    return offset / 8 + 1;
#else
    return ptr >> 3;
#endif
}

template<typename Method>
size_t GetMethodIndex(Method method) {
    uintptr_t ptr;
    memcpy(&ptr, &method, sizeof(uintptr_t));
    return GetMethodPtrIndex(ptr);
}

template<bool HasLength = true>
class TLazyList: public NUdf::TBoxedValue {
    struct TIterator: public NUdf::TBoxedValue {
        TIterator(i32 from, i32 to)
            : From(from), To(to), Curr(Max<i32>())
        {
            if (To >= From) {
                To--; // exclude last
            } else {
                From--; // exclude first
            }
        }
    private:
        bool Skip() override {
            if (Curr == Max<i32>()) {
                Curr = From;
                return true;
            }
            if (To >= From) {
                if (Curr < To) {
                    ++Curr;
                    return true;
                }
            } else {
                if (Curr > To) {
                    --Curr;
                    return true;
                }
            }
            return false;
        }

        bool Next(NUdf::TUnboxedValue& value) override {
            if (!Skip())
                return false;
            value = NUdf::TUnboxedValuePod(Curr);
            return true;
        }
        i32 From, To, Curr;
    };
public:
    TLazyList(i32 from, i32 to)
        : From_(from), To_(to)
    {
    }

private:
    bool HasFastListLength() const override {
        return HasLength;
    }

    ui64 GetListLength() const override {
        if (HasLength)
            return Abs(To_ - From_);

        Y_FAIL("No length!");
    }

    bool HasListItems() const override {
        return To_ != From_;
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(From_, To_));
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return new TLazyList(To_, From_);
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        Y_UNUSED(builder);
        count = std::min<ui64>(count, Abs(To_ - From_));
        if (To_ >= From_) {
            return new TLazyList(From_ + count, To_);
        }
        return new TLazyList(From_ - count, To_);
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        Y_UNUSED(builder);
        count = std::min<ui64>(count, Abs(To_ - From_));
        if (To_ >= From_) {
            return new TLazyList(From_, From_ + count);
        }
        return new TLazyList(From_, From_ - count);
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return nullptr;
    }

    i32 From_, To_;
};

}
}

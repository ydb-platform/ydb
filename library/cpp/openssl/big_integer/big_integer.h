#pragma once

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/utility.h>
#include <util/generic/string.h>

struct bignum_st;

namespace NOpenSsl {
    class TBigInteger {
        inline TBigInteger(bignum_st* impl) noexcept
            : Impl_(impl)
        {
        }

        static int Compare(const TBigInteger& a, const TBigInteger& b) noexcept;

    public:
        inline TBigInteger(TBigInteger&& other) noexcept {
            Swap(other);
        }

        ~TBigInteger() noexcept;

        static TBigInteger FromULong(ui64 value);
        static TBigInteger FromRegion(const void* ptr, size_t len);

        inline const bignum_st* Impl() const noexcept {
            return Impl_;
        }

        inline bignum_st* Impl() noexcept {
            return Impl_;
        }

        inline void Swap(TBigInteger& other) noexcept {
            DoSwap(Impl_, other.Impl_);
        }

        inline friend bool operator==(const TBigInteger& a, const TBigInteger& b) noexcept {
            return Compare(a, b) == 0;
        }

        inline friend bool operator!=(const TBigInteger& a, const TBigInteger& b) noexcept {
            return !(a == b);
        }

        size_t NumBytes() const noexcept;
        size_t ToRegion(void* to) const noexcept;

        TString ToDecimalString() const;

    private:
        bignum_st* Impl_ = nullptr;
    };
}

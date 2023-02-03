#pragma once

#include "udf_type_size_check.h"

#include <util/generic/strbuf.h>

#include <algorithm>
#include <string_view>
#include <type_traits>

namespace NYql {
namespace NUdf {

//////////////////////////////////////////////////////////////////////////////
// TStringRefBase
//////////////////////////////////////////////////////////////////////////////
template<bool Const>
class TStringRefBase
{
public:
    typedef std::conditional_t<Const, const char*, char*> TDataType;

protected:
    inline constexpr TStringRefBase() noexcept = default;

    inline constexpr TStringRefBase(TDataType data, ui32 size) noexcept
        : Data_(data)
        , Size_(size)
    {}

public:
    inline constexpr operator std::string_view() const noexcept { return { Data_, Size_ }; }
    inline constexpr operator TStringBuf() const noexcept { return { Data_, Size_ }; }
    inline constexpr TDataType Data() const noexcept { return Data_; }
    inline constexpr ui32 Size() const noexcept { return Size_; }
    inline constexpr bool Empty() const noexcept { return Size_ == 0; }

protected:
    TDataType Data_ = nullptr;
    ui32 Size_ = 0U;
    ui8 Reserved_[4] = {};
};

//////////////////////////////////////////////////////////////////////////////
// TMutableStringRef
//////////////////////////////////////////////////////////////////////////////
class TMutableStringRef : public TStringRefBase<false>
{
public:
    typedef TStringRefBase<false> TBase;

    inline constexpr TMutableStringRef(TDataType data, ui32 size) noexcept
        : TBase(data, size)
    {}
};

UDF_ASSERT_TYPE_SIZE(TMutableStringRef, 16);

//////////////////////////////////////////////////////////////////////////////
// TStringRef
//////////////////////////////////////////////////////////////////////////////
class TStringRef : public TStringRefBase<true>
{
public:
    typedef TStringRefBase<true> TBase;

    inline constexpr TStringRef() noexcept = default;

    inline constexpr TStringRef(TDataType data, ui32 size) noexcept
        : TBase(data, size)
    {}

    template<size_t Size>
    inline constexpr TStringRef(const char (&data)[Size]) noexcept
        : TBase(data, Size - 1)
    {}

    inline constexpr TStringRef(const TMutableStringRef& buf) noexcept
        : TBase(buf.Data(), buf.Size())
    {}

    template <typename TStringType>
    inline constexpr TStringRef(const TStringType& buf) noexcept
        : TBase(TGetData<TStringType>::Get(buf), TGetSize<TStringType>::Get(buf))
    {}

    template <size_t size>
    inline static constexpr TStringRef Of(const char(&str)[size]) noexcept {
        return TStringRef(str);
    }

    inline constexpr TStringRef& Trunc(ui32 len) noexcept {
        if (Size_ > len) {
            Size_ = len;
        }
        return *this;
    }

    inline constexpr TStringRef Substring(ui32 start, ui32 count) const noexcept {
        start = std::min(start, Size_);
        count = std::min(count, Size_ - start);
        return TStringRef(Data_ + start, count);
    }

    inline constexpr bool operator==(const TStringRef& rhs) const noexcept {
        return Compare(*this, rhs) == 0;
    }

    inline constexpr bool operator!=(const TStringRef& rhs) const noexcept {
        return Compare(*this, rhs) != 0;
    }

    inline constexpr bool operator<(const TStringRef& rhs) const noexcept {
        return Compare(*this, rhs) < 0;
    }

    inline constexpr bool operator<=(const TStringRef& rhs) const noexcept {
        return Compare(*this, rhs) <= 0;
    }

    inline constexpr bool operator>(const TStringRef& rhs) const noexcept {
        return Compare(*this, rhs) > 0;
    }

    inline constexpr bool operator>=(const TStringRef& rhs) const noexcept {
        return Compare(*this, rhs) >= 0;
    }

    inline constexpr i64 Compare(const TStringRef& rhs) const noexcept {
        return Compare(*this, rhs);
    }

private:
    inline static constexpr i64 Compare(const TStringRef& s1, const TStringRef& s2) noexcept {
        auto minSize = std::min(s1.Size(), s2.Size());
        if (const auto result = minSize > 0 ? std::memcmp(s1.Data(), s2.Data(), minSize) : 0)
            return result;
        return i64(s1.Size()) - i64(s2.Size());
    }

    Y_HAS_MEMBER(Data);
    Y_HAS_MEMBER(Size);

    template<typename TStringType>
    struct TByData {
        static constexpr auto Get(const TStringType& buf) noexcept {
            return buf.Data();
        }
    };

    template<typename TStringType>
    struct TBySize {
        static constexpr auto Get(const TStringType& buf) noexcept {
            return buf.Size();
        }
    };

    template<typename TStringType>
    struct TBydata {
        static constexpr auto Get(const TStringType& buf) noexcept {
            return buf.data();
        }
    };

    template<typename TStringType>
    struct TBysize {
        static constexpr auto Get(const TStringType& buf) noexcept {
            return buf.size();
        }
    };

    template<typename TStringType>
    using TGetData = std::conditional_t<THasData<TStringType>::value, TByData<TStringType>, TBydata<TStringType>>;

    template<typename TStringType>
    using TGetSize = std::conditional_t<THasSize<TStringType>::value, TBySize<TStringType>, TBysize<TStringType>>;
};

UDF_ASSERT_TYPE_SIZE(TStringRef, 16);

} // namspace NUdf
} // namspace NYql

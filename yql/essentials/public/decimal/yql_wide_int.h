#pragma once

#include <util/system/types.h>

#include <type_traits>
#include <tuple>
#include <cmath>
#include <compare>
#include <array>

namespace NYql {

#ifndef _win_
typedef __int128 i128_t;
typedef unsigned __int128 ui128_t;
#endif

template <typename TOneHalf, typename TSignedPart = std::make_signed_t<TOneHalf>, typename TUnsignedPart = std::make_unsigned_t<TOneHalf>>
class TWide {
private:
    using THalf = TOneHalf;
    using TPart = TUnsignedPart;

    using TIsSigned = std::is_same<THalf, TSignedPart>;
    using TIsUnsigned = std::is_same<THalf, TUnsignedPart>;

    static_assert(TIsSigned::value || TIsUnsigned::value, "Invalid using of TWide.");
    static_assert(sizeof(TSignedPart) == sizeof(TUnsignedPart), "Different sizes of TWide parts.");

    TPart Lo_;
    THalf Hi_;

    static constexpr auto FullByteSize = sizeof(TPart) * 2;
    static constexpr auto FullBitSize = FullByteSize * 8;
    static constexpr auto PartBitSize = sizeof(TPart) << 3U;
    static constexpr auto QuarterBitSize = sizeof(TPart) << 2U;

    template <typename T>
    static constexpr bool IsArithmeticSuitable = (std::is_integral_v<T> && sizeof(T) <= FullByteSize) || std::is_floating_point_v<T>;

    static constexpr TPart GetUpperQuarter(TPart h) {
        return h >> QuarterBitSize;
    }

    static constexpr TPart GetLowerQuarter(TPart h) {
        const auto mask = TPart(~TPart(0U)) >> QuarterBitSize;
        return h & mask;
    }

    template <typename T>
    constexpr std::enable_if_t<sizeof(T) <= sizeof(THalf), T> CastImpl() const {
        return static_cast<T>(Lo_);
    }

    template <typename T>
    constexpr std::enable_if_t<sizeof(T) == sizeof(THalf) << 1U, T> CastImpl() const {
        return *reinterpret_cast<const T*>(this);
    }

    template <typename T>
    constexpr std::enable_if_t<sizeof(THalf) << 1U < sizeof(T), T> CastImpl() const {
        return (T(std::make_unsigned_t<T>(Hi_) << PartBitSize)) | Lo_;
    }

    constexpr size_t GetBits() const {
        size_t out = Hi_ ? PartBitSize : 0U;
        for (auto up = TPart(out ? Hi_ : Lo_); up; up >>= 1U) {
            ++out;
        }
        return out;
    }

    using TUnsigned = TWide<TUnsignedPart, TSignedPart, TUnsignedPart>;

    using TSibling = std::conditional_t<std::is_same<THalf, TPart>::value,
                                        TWide<TSignedPart, TSignedPart, TUnsignedPart>, TWide<TUnsignedPart, TSignedPart, TUnsignedPart>>;
    friend TSibling;

public:
    constexpr TWide() = default;
    constexpr TWide(const TWide& rhs) = default;
    constexpr TWide(TWide&& rhs) = default;

    constexpr TWide& operator=(const TWide& rhs) = default;
    constexpr TWide& operator=(TWide&& rhs) = default;

    // Implicit casts between numbers is not surprising
    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr TWide(const TSibling& rhs)
        : Lo_(rhs.Lo_)
        , Hi_(rhs.Hi_)
    {
    }

    template <typename U, typename L>
    constexpr TWide(const U upper_rhs, const L lower_rhs)
        : Lo_(lower_rhs)
        , Hi_(upper_rhs)
    {
    }

    template <typename T, std::enable_if_t<std::is_integral<T>::value && sizeof(THalf) < sizeof(T), size_t> Shift = PartBitSize>
    constexpr TWide(const T rhs) // NOLINT(google-explicit-constructor)
        : Lo_(rhs)
        , Hi_(rhs >> Shift)
    {
    }

    template <typename T, std::enable_if_t<std::is_integral<T>::value && sizeof(T) <= sizeof(THalf), bool> Signed = std::is_signed<T>::value>
    constexpr TWide(const T rhs) // NOLINT(google-explicit-constructor)
        : Lo_(rhs)
        , Hi_(Signed && rhs < 0 ? ~0 : 0)
    {
    }

    template <typename T, typename TArg = std::enable_if_t<std::is_class<T>::value && std::is_same<T, THalf>::value, THalf>>
    constexpr TWide(const T& rhs) // NOLINT(google-explicit-constructor)
        : Lo_(rhs)
        , Hi_(TIsSigned::value && rhs < 0 ? ~0 : 0)
    {
    }

    template <typename T, std::enable_if_t<std::is_integral<T>::value && sizeof(THalf) < sizeof(T), size_t> Shift = PartBitSize>
    constexpr TWide& operator=(const T rhs) {
        Hi_ = rhs >> Shift;
        Lo_ = rhs;
        return *this;
    }

    template <typename T, std::enable_if_t<std::is_integral<T>::value && sizeof(T) <= sizeof(THalf), bool> Signed = std::is_signed<T>::value>
    constexpr TWide& operator=(const T rhs) {
        Hi_ = Signed && rhs < 0 ? ~0 : 0;
        Lo_ = rhs;
        return *this;
    }

    constexpr explicit operator bool() const {
        return bool(Hi_) || bool(Lo_);
    }

    constexpr explicit operator i8() const {
        return CastImpl<i8>();
    }
    constexpr explicit operator ui8() const {
        return CastImpl<ui8>();
    }
    constexpr explicit operator i16() const {
        return CastImpl<i16>();
    }
    constexpr explicit operator ui16() const {
        return CastImpl<ui16>();
    }
    constexpr explicit operator i32() const {
        return CastImpl<i32>();
    }
    constexpr explicit operator ui32() const {
        return CastImpl<ui32>();
    }
    constexpr explicit operator i64() const {
        return CastImpl<i64>();
    }
    constexpr explicit operator ui64() const {
        return CastImpl<ui64>();
    }
#ifndef _win_
    constexpr explicit operator i128_t() const {
        return CastImpl<i128_t>();
    }
    constexpr explicit operator ui128_t() const {
        return CastImpl<ui128_t>();
    }
#endif
    constexpr explicit operator float() const {
        return TIsSigned::value && Hi_ < 0 ? -float(-*this) : std::fma(float(Hi_), std::exp2(float(PartBitSize)), float(Lo_));
    }

    constexpr explicit operator double() const {
        return TIsSigned::value && Hi_ < 0 ? -double(-*this) : std::fma(double(Hi_), std::exp2(double(PartBitSize)), double(Lo_));
    }

    constexpr TWide operator~() const {
        return TWide(~Hi_, ~Lo_);
    }

    constexpr TWide operator+() const {
        return TWide(Hi_, Lo_);
    }

    constexpr TWide operator-() const {
        const auto sign = THalf(1) << PartBitSize - 1U;
        if (TIsSigned::value && !Lo_ && Hi_ == sign) {
            return *this;
        }
        return ++~*this;
    }

    constexpr TWide& operator++() {
        if (!++Lo_) {
            ++Hi_;
        }
        return *this;
    }

    constexpr TWide& operator--() {
        if (!Lo_--) {
            --Hi_;
        }
        return *this;
    }

    constexpr TWide operator++(int) {
        const TWide r(*this);
        ++*this;
        return r;
    }

    constexpr TWide operator--(int) {
        const TWide r(*this);
        --*this;
        return r;
    }

    constexpr TWide& operator&=(const TWide& rhs) {
        Hi_ &= rhs.Hi_;
        Lo_ &= rhs.Lo_;
        return *this;
    }

    constexpr TWide& operator|=(const TWide& rhs) {
        Hi_ |= rhs.Hi_;
        Lo_ |= rhs.Lo_;
        return *this;
    }

    constexpr TWide& operator^=(const TWide& rhs) {
        Hi_ ^= rhs.Hi_;
        Lo_ ^= rhs.Lo_;
        return *this;
    }

    constexpr TWide& operator+=(const TWide& rhs) {
        const auto l = Lo_;
        Lo_ += rhs.Lo_;
        Hi_ += rhs.Hi_;
        if (l > Lo_) {
            ++Hi_;
        }
        return *this;
    }

    constexpr TWide& operator-=(const TWide& rhs) {
        const auto l = Lo_;
        Lo_ -= rhs.Lo_;
        Hi_ -= rhs.Hi_;
        if (l < Lo_) {
            --Hi_;
        }
        return *this;
    }

    constexpr TWide& operator<<=(const TWide& rhs) {
        if (const auto shift = size_t(rhs.Lo_) % FullBitSize) {
            if (shift < PartBitSize) {
                Hi_ = TPart(Hi_) << shift;
                Hi_ |= Lo_ >> (PartBitSize - shift);
                Lo_ <<= shift;
            } else {
                Hi_ = Lo_ << (shift - PartBitSize);
                Lo_ = 0;
            }
        }

        return *this;
    }

    constexpr TWide& operator>>=(const TWide& rhs) {
        if (const auto shift = size_t(rhs.Lo_) % FullBitSize) {
            if (shift < PartBitSize) {
                Lo_ >>= shift;
                Lo_ |= TPart(Hi_) << (PartBitSize - shift);
                Hi_ >>= shift;
            } else {
                Lo_ = Hi_ >> (shift - PartBitSize);
                Hi_ = TIsSigned::value && Hi_ < 0 ? ~0 : 0;
            }
        }

        return *this;
    }

    constexpr TWide& operator*=(const TWide& rhs) {
        return *this = Mul(*this, rhs);
    }
    constexpr TWide& operator/=(const TWide& rhs) {
        return *this = DivMod(*this, rhs).first;
    }
    constexpr TWide& operator%=(const TWide& rhs) {
        return *this = DivMod(*this, rhs).second;
    }

    constexpr TWide operator&(const TWide& rhs) const {
        return TWide(*this) &= rhs;
    }
    constexpr TWide operator|(const TWide& rhs) const {
        return TWide(*this) |= rhs;
    }
    constexpr TWide operator^(const TWide& rhs) const {
        return TWide(*this) ^= rhs;
    }

    constexpr TWide operator*(const TWide& rhs) const {
        return Mul(*this, rhs);
    }
    constexpr TWide operator/(const TWide& rhs) const {
        return DivMod(*this, rhs).first;
    }
    constexpr TWide operator%(const TWide& rhs) const {
        return DivMod(*this, rhs).second;
    }
    constexpr TWide operator<<(const TWide& rhs) const {
        return TWide(*this) <<= rhs;
    }
    constexpr TWide operator>>(const TWide& rhs) const {
        return TWide(*this) >>= rhs;
    }

    template <typename T>
    constexpr std::enable_if_t<std::is_integral<T>::value, T> operator&(const T rhs) const {
        return T(*this) & rhs;
    }

    friend constexpr TWide operator+(const TWide& lhs, const TWide& rhs) {
        return TWide(lhs) += rhs;
    }

    friend constexpr TWide operator-(const TWide& lhs, const TWide& rhs) {
        return TWide(lhs) -= rhs;
    }

    template <typename U>
        requires IsArithmeticSuitable<U>
    friend constexpr auto operator+(const TWide& lhs, const U& rhs) {
        if constexpr (std::is_floating_point_v<U>) {
            return static_cast<U>(lhs) + rhs;
        } else {
            return TWide(lhs) += TWide(rhs);
        }
    }

    template <typename U>
        requires IsArithmeticSuitable<U>
    friend constexpr auto operator+(const U& lhs, const TWide& rhs) {
        if constexpr (std::is_floating_point_v<U>) {
            return lhs + static_cast<U>(rhs);
        } else {
            return TWide(lhs) += rhs;
        }
    }

    template <typename U>
        requires IsArithmeticSuitable<U>
    friend constexpr auto operator-(const TWide& lhs, const U& rhs) {
        if constexpr (std::is_floating_point_v<U>) {
            return static_cast<U>(lhs) - rhs;
        } else {
            return TWide(lhs) -= TWide(rhs);
        }
    }

    template <typename U>
        requires IsArithmeticSuitable<U>
    friend constexpr auto operator-(const U& lhs, const TWide& rhs) {
        if constexpr (std::is_floating_point_v<U>) {
            return lhs - static_cast<U>(rhs);
        } else {
            return TWide(lhs) -= rhs;
        }
    }

    friend constexpr bool operator==(const TWide& lhs, const TWide& rhs) = default;

    friend constexpr auto operator<=>(const TWide& lhs, const TWide& rhs) {
        return std::tie(lhs.Hi_, lhs.Lo_) <=> std::tie(rhs.Hi_, rhs.Lo_);
    }

    template <typename U>
        requires IsArithmeticSuitable<U>
    friend constexpr bool operator==(const TWide& lhs, const U& rhs) {
        if constexpr (std::is_floating_point_v<U>) {
            return static_cast<U>(lhs) == rhs;
        } else {
            return lhs == TWide(rhs);
        }
    }

    template <typename U>
        requires IsArithmeticSuitable<U>
    friend constexpr bool operator==(const U& lhs, const TWide& rhs) {
        if constexpr (std::is_floating_point_v<U>) {
            return lhs == static_cast<U>(rhs);
        } else {
            return TWide(lhs) == rhs;
        }
    }

    template <typename U>
        requires IsArithmeticSuitable<U>
    friend constexpr auto operator<=>(const TWide& lhs, const U& rhs) {
        if constexpr (std::is_floating_point_v<U>) {
            return static_cast<U>(lhs) <=> rhs;
        } else {
            return lhs <=> TWide(rhs);
        }
    }

    template <typename U>
        requires IsArithmeticSuitable<U>
    friend constexpr auto operator<=>(const U& lhs, const TWide& rhs) {
        if constexpr (std::is_floating_point_v<U>) {
            return lhs <=> static_cast<U>(rhs);
        } else {
            return TWide(lhs) <=> rhs;
        }
    }

private:
    static constexpr TWide Mul(const TWide& lhs, const TWide& rhs) {
        const auto lq = std::to_array<TPart>({GetLowerQuarter(lhs.Lo_), GetUpperQuarter(lhs.Lo_), GetLowerQuarter(lhs.Hi_), GetUpperQuarter(lhs.Hi_)});
        const auto rq = std::to_array<TPart>({GetLowerQuarter(rhs.Lo_), GetUpperQuarter(rhs.Lo_), GetLowerQuarter(rhs.Hi_), GetUpperQuarter(rhs.Hi_)});

        const auto prod0 = std::to_array<TPart>({TPart(lq[0] * rq[0])});
        const auto prod1 = std::to_array<TPart>({TPart(lq[0] * rq[1]), TPart(lq[1] * rq[0])});
        const auto prod2 = std::to_array<TPart>({TPart(lq[0] * rq[2]), TPart(lq[1] * rq[1]), TPart(lq[2] * rq[0])});
        const auto prod3 = std::to_array<TPart>({TPart(lq[0] * rq[3]), TPart(lq[1] * rq[2]), TPart(lq[2] * rq[1]), TPart(lq[3] * rq[0])});

        const TPart fourthQ = GetLowerQuarter(prod0[0]);
        const TPart thirdQ = GetUpperQuarter(prod0[0]) + GetLowerQuarter(prod1[0]) + GetLowerQuarter(prod1[1]);
        const TPart secondQ = GetUpperQuarter(thirdQ) + GetUpperQuarter(prod1[0]) + GetUpperQuarter(prod1[1]) + GetLowerQuarter(prod2[0]) + GetLowerQuarter(prod2[1]) + GetLowerQuarter(prod2[2]);
        const TPart firstQ = GetUpperQuarter(secondQ) + GetUpperQuarter(prod2[0]) + GetUpperQuarter(prod2[1]) + GetUpperQuarter(prod2[2]) + GetLowerQuarter(prod3[0]) + GetLowerQuarter(prod3[1]) + GetLowerQuarter(prod3[2]) + GetLowerQuarter(prod3[3]);

        return TWide((firstQ << QuarterBitSize) | GetLowerQuarter(secondQ), (thirdQ << QuarterBitSize) | fourthQ);
    }

    static constexpr std::pair<TWide, TWide> DivMod(const TWide& lhs, const TWide& rhs) {
        const bool nl = TIsSigned::value && lhs.Hi_ < 0;
        const bool nr = TIsSigned::value && rhs.Hi_ < 0;

        const TUnsigned l = nl ? -lhs : +lhs, r = nr ? -rhs : +rhs;

        TUnsigned div = 0, mod = 0;

        for (auto x = l.GetBits(); x;) {
            mod <<= 1;
            div <<= 1;

            if (--x < PartBitSize ? l.Lo_ & (TPart(1U) << x) : l.Hi_ & (TPart(1U) << x - PartBitSize)) {
                ++mod;
            }

            if (mod >= r) {
                mod -= r;
                ++div;
            }
        }

        if (nl) {
            mod = -mod;
        }
        if (nr != nl) {
            div = -div;
        }

        return {div, mod};
    }
};

template <typename T>
struct THalfOf;
template <>
struct THalfOf<i16> {
    typedef i8 Type;
};
template <>
struct THalfOf<ui16> {
    typedef ui8 Type;
};
template <>
struct THalfOf<i32> {
    typedef i16 Type;
};
template <>
struct THalfOf<ui32> {
    typedef ui16 Type;
};
template <>
struct THalfOf<i64> {
    typedef i32 Type;
};
template <>
struct THalfOf<ui64> {
    typedef ui32 Type;
};
#ifndef _win_
template <>
struct THalfOf<i128_t> {
    typedef i64 Type;
};
template <>
struct THalfOf<ui128_t> {
    typedef ui64 Type;
};
#endif
template <typename T>
struct THalfOf {};

template <typename T>
struct TPairOf;
template <>
struct TPairOf<i8> {
    typedef i16 Type;
};
template <>
struct TPairOf<ui8> {
    typedef ui16 Type;
};
template <>
struct TPairOf<i16> {
    typedef i32 Type;
};
template <>
struct TPairOf<ui16> {
    typedef ui32 Type;
};
template <>
struct TPairOf<i32> {
    typedef i64 Type;
};
template <>
struct TPairOf<ui32> {
    typedef ui64 Type;
};
#ifndef _win_
template <>
struct TPairOf<i64> {
    typedef i128_t Type;
};
template <>
struct TPairOf<ui64> {
    typedef ui128_t Type;
};
#endif
template <typename T>
struct TPairOf {};

} // namespace NYql

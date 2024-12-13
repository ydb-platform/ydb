#pragma once

#include <util/system/types.h>

#include <type_traits>
#include <tuple>
#include <cmath>

namespace NYql {

#ifndef _win_
typedef __int128 i128_t;
typedef unsigned __int128 ui128_t;
#endif

template<typename TOneHalf, typename TSignedPart = std::make_signed_t<TOneHalf>, typename TUnsignedPart = std::make_unsigned_t<TOneHalf>>
class TWide {
private:
    using THalf = TOneHalf;
    using TPart = TUnsignedPart;

    using TIsSigned = std::is_same<THalf, TSignedPart>;
    using TIsUnsigned = std::is_same<THalf, TUnsignedPart>;

    static_assert(TIsSigned::value || TIsUnsigned::value, "Invalid using of TWide.");
    static_assert(sizeof(TSignedPart) == sizeof(TUnsignedPart), "Different sizes of TWide parts.");

    TPart Lo;
    THalf Hi;

    static constexpr auto FullBitSize = sizeof(TPart) << 4U;
    static constexpr auto PartBitSize = sizeof(TPart) << 3U;
    static constexpr auto QuarterBitSize = sizeof(TPart) << 2U;

    static constexpr TPart GetUpperQuarter(TPart h) {
        return h >> QuarterBitSize;
    }

    static constexpr TPart GetLowerQuarter(TPart h) {
        const auto mask = TPart(~TPart(0U)) >> QuarterBitSize;
        return h & mask;
    }

    template<typename T>
    constexpr std::enable_if_t<sizeof(T) <= sizeof(THalf), T> CastImpl() const {
        return static_cast<T>(Lo);
    }

    template<typename T>
    constexpr std::enable_if_t<sizeof(T) == sizeof(THalf) << 1U, T> CastImpl() const {
        return *reinterpret_cast<const T*>(this);
    }

    template<typename T>
    constexpr std::enable_if_t<sizeof(THalf) << 1U < sizeof(T), T> CastImpl() const {
        return (T(std::make_unsigned_t<T>(Hi) << PartBitSize)) | Lo;
    }

    constexpr size_t GetBits() const {
        size_t out = Hi ? PartBitSize : 0U;
        for (auto up = TPart(out ? Hi : Lo); up; up >>= 1U) {
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

    constexpr TWide(const TSibling& rhs)
        : Lo(rhs.Lo), Hi(rhs.Hi)
    {}

    template<typename U, typename L>
    constexpr TWide(const U upper_rhs, const L lower_rhs)
        : Lo(lower_rhs), Hi(upper_rhs)
    {}

    template <typename T, std::enable_if_t<std::is_integral<T>::value && sizeof(THalf) < sizeof(T), size_t> Shift = PartBitSize>
    constexpr TWide(const T rhs)
        : Lo(rhs), Hi(rhs >> Shift)
    {}

    template <typename T, std::enable_if_t<std::is_integral<T>::value && sizeof(T) <= sizeof(THalf), bool> Signed = std::is_signed<T>::value>
    constexpr TWide(const T rhs)
        : Lo(rhs), Hi(Signed && rhs < 0 ? ~0 : 0)
    {}

    template <typename T, typename TArg = std::enable_if_t<std::is_class<T>::value && std::is_same<T, THalf>::value, THalf>>
    constexpr explicit TWide(const T& rhs)
        : Lo(rhs), Hi(TIsSigned::value && rhs < 0 ? ~0 : 0)
    {}

    template <typename T, std::enable_if_t<std::is_integral<T>::value && sizeof(THalf) < sizeof(T), size_t> Shift = PartBitSize>
    constexpr TWide& operator=(const T rhs) {
        Hi = rhs >> Shift;
        Lo = rhs;
        return *this;
    }

    template <typename T, std::enable_if_t<std::is_integral<T>::value && sizeof(T) <= sizeof(THalf), bool> Signed = std::is_signed<T>::value>
    constexpr TWide& operator=(const T rhs) {
        Hi = Signed && rhs < 0 ? ~0 : 0;
        Lo = rhs;
        return *this;
    }

    constexpr explicit operator bool() const { return bool(Hi) || bool(Lo); }

    constexpr explicit operator i8()   const { return CastImpl<i8>(); }
    constexpr explicit operator ui8()  const { return CastImpl<ui8>(); }
    constexpr explicit operator i16()  const { return CastImpl<i16>(); }
    constexpr explicit operator ui16() const { return CastImpl<ui16>(); }
    constexpr explicit operator i32()  const { return CastImpl<i32>(); }
    constexpr explicit operator ui32() const { return CastImpl<ui32>(); }
    constexpr explicit operator i64()  const { return CastImpl<i64>(); }
    constexpr explicit operator ui64() const { return CastImpl<ui64>(); }
#ifndef _win_
    constexpr explicit operator i128_t()  const { return CastImpl<i128_t>(); }
    constexpr explicit operator ui128_t() const { return CastImpl<ui128_t>(); }
#endif
    constexpr explicit operator float() const {
        return TIsSigned::value && Hi < 0 ? -float(-*this) : std::fma(float(Hi), std::exp2(float(PartBitSize)), float(Lo));
    }

    constexpr explicit operator double() const {
        return TIsSigned::value && Hi < 0 ? -double(-*this) : std::fma(double(Hi), std::exp2(double(PartBitSize)), double(Lo));
    }

    constexpr TWide operator~() const {
        return TWide(~Hi, ~Lo);
    }

    constexpr TWide operator+() const {
        return TWide(Hi, Lo);
    }

    constexpr TWide operator-() const {
        const auto sign = THalf(1) << PartBitSize - 1U;
        if (TIsSigned::value && !Lo && Hi == sign)
            return *this;
        return ++~*this;
    }

    constexpr TWide& operator++() {
        if (!++Lo) ++Hi;
        return *this;
    }

    constexpr TWide& operator--() {
        if (!Lo--) --Hi;
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
        Hi &= rhs.Hi;
        Lo &= rhs.Lo;
        return *this;
    }

    constexpr TWide& operator|=(const TWide& rhs) {
        Hi |= rhs.Hi;
        Lo |= rhs.Lo;
        return *this;
    }

    constexpr TWide& operator^=(const TWide& rhs) {
        Hi ^= rhs.Hi;
        Lo ^= rhs.Lo;
        return *this;
    }

    constexpr TWide& operator+=(const TWide& rhs) {
        const auto l = Lo;
        Lo += rhs.Lo;
        Hi += rhs.Hi;
        if (l > Lo) ++Hi;
        return *this;
    }

    constexpr TWide& operator-=(const TWide& rhs) {
        const auto l = Lo;
        Lo -= rhs.Lo;
        Hi -= rhs.Hi;
        if (l < Lo) --Hi;
        return *this;
    }

    constexpr TWide& operator<<=(const TWide& rhs) {
        if (const auto shift = size_t(rhs.Lo) % FullBitSize) {
            if (shift < PartBitSize) {
                Hi = TPart(Hi) << shift;
                Hi |= Lo >> (PartBitSize - shift);
                Lo <<= shift;
            } else {
                Hi = Lo << (shift - PartBitSize);
                Lo = 0;
            }
        }

        return *this;
    }

    constexpr TWide& operator>>=(const TWide& rhs) {
        if (const auto shift = size_t(rhs.Lo) % FullBitSize) {
            if (shift < PartBitSize) {
                Lo >>= shift;
                Lo |= TPart(Hi) << (PartBitSize - shift);
                Hi >>= shift;
            } else {
                Lo = Hi >> (shift - PartBitSize);
                Hi = TIsSigned::value && Hi < 0 ? ~0 : 0;
            }
        }

        return *this;
    }

    constexpr TWide& operator*=(const TWide& rhs) { return *this = Mul(*this, rhs); }
    constexpr TWide& operator/=(const TWide& rhs) { return *this = DivMod(*this, rhs).first; }
    constexpr TWide& operator%=(const TWide& rhs) { return *this = DivMod(*this, rhs).second; }

    constexpr TWide operator&(const TWide& rhs) const { return TWide(*this) &= rhs; }
    constexpr TWide operator|(const TWide& rhs) const { return TWide(*this) |= rhs; }
    constexpr TWide operator^(const TWide& rhs) const { return TWide(*this) ^= rhs; }
    constexpr TWide operator+(const TWide& rhs) const { return TWide(*this) += rhs; }
    constexpr TWide operator-(const TWide& rhs) const { return TWide(*this) -= rhs; }
    constexpr TWide operator*(const TWide& rhs) const { return Mul(*this, rhs); }
    constexpr TWide operator/(const TWide& rhs) const { return DivMod(*this, rhs).first; }
    constexpr TWide operator%(const TWide& rhs) const { return DivMod(*this, rhs).second; }
    constexpr TWide operator<<(const TWide& rhs) const { return TWide(*this) <<= rhs; }
    constexpr TWide operator>>(const TWide& rhs) const { return TWide(*this) >>= rhs; }

    template <typename T>
    constexpr std::enable_if_t<std::is_integral<T>::value, T> operator&(const T rhs) const { return T(*this) & rhs; }

    constexpr bool operator==(const TWide& rhs) const { return std::tie(Hi, Lo) == std::tie(rhs.Hi, rhs.Lo); }
    constexpr bool operator!=(const TWide& rhs) const { return std::tie(Hi, Lo) != std::tie(rhs.Hi, rhs.Lo); }
    constexpr bool operator>=(const TWide& rhs) const { return std::tie(Hi, Lo) >= std::tie(rhs.Hi, rhs.Lo); }
    constexpr bool operator<=(const TWide& rhs) const { return std::tie(Hi, Lo) <= std::tie(rhs.Hi, rhs.Lo); }
    constexpr bool operator>(const TWide& rhs) const  { return std::tie(Hi, Lo) > std::tie(rhs.Hi, rhs.Lo); }
    constexpr bool operator<(const TWide& rhs) const  { return std::tie(Hi, Lo) < std::tie(rhs.Hi, rhs.Lo); }

private:
    static constexpr TWide Mul(const TWide& lhs, const TWide& rhs) {
        const TPart lq[] = {GetLowerQuarter(lhs.Lo), GetUpperQuarter(lhs.Lo), GetLowerQuarter(lhs.Hi), GetUpperQuarter(lhs.Hi)};
        const TPart rq[] = {GetLowerQuarter(rhs.Lo), GetUpperQuarter(rhs.Lo), GetLowerQuarter(rhs.Hi), GetUpperQuarter(rhs.Hi)};

        const TPart prod0[] = {TPart(lq[0] * rq[0])};
        const TPart prod1[] = {TPart(lq[0] * rq[1]), TPart(lq[1] * rq[0])};
        const TPart prod2[] = {TPart(lq[0] * rq[2]), TPart(lq[1] * rq[1]), TPart(lq[2] * rq[0])};
        const TPart prod3[] = {TPart(lq[0] * rq[3]), TPart(lq[1] * rq[2]), TPart(lq[2] * rq[1]), TPart(lq[3] * rq[0])};

        const TPart fourthQ = GetLowerQuarter(prod0[0]);
        const TPart thirdQ = GetUpperQuarter(prod0[0])
            + GetLowerQuarter(prod1[0]) + GetLowerQuarter(prod1[1]);
        const TPart secondQ = GetUpperQuarter(thirdQ)
            + GetUpperQuarter(prod1[0]) + GetUpperQuarter(prod1[1])
            + GetLowerQuarter(prod2[0]) + GetLowerQuarter(prod2[1]) + GetLowerQuarter(prod2[2]);
        const TPart firstQ = GetUpperQuarter(secondQ)
            + GetUpperQuarter(prod2[0]) + GetUpperQuarter(prod2[1]) + GetUpperQuarter(prod2[2])
            + GetLowerQuarter(prod3[0]) + GetLowerQuarter(prod3[1]) + GetLowerQuarter(prod3[2]) + GetLowerQuarter(prod3[3]);

        return TWide((firstQ << QuarterBitSize) | GetLowerQuarter(secondQ), (thirdQ << QuarterBitSize) | fourthQ);
    }

    static constexpr std::pair<TWide, TWide> DivMod(const TWide& lhs, const TWide& rhs) {
        const bool nl = TIsSigned::value && lhs.Hi < 0;
        const bool nr = TIsSigned::value && rhs.Hi < 0;

        const TUnsigned l = nl ? -lhs : +lhs, r = nr ? -rhs : +rhs;

        TUnsigned div = 0, mod = 0;

        for (auto x = l.GetBits(); x;) {
            mod <<= 1;
            div <<= 1;

            if (--x < PartBitSize ? l.Lo & (TPart(1U) << x) : l.Hi & (TPart(1U) << x - PartBitSize)) {
                ++mod;
            }

            if (mod >= r) {
                mod -= r;
                ++div;
            }
        }

        if (nl) mod = -mod;
        if (nr != nl) div = -div;

        return {div, mod};
    }
};

template<typename T> struct THalfOf;
template<> struct THalfOf<i16>  { typedef i8 Type; };
template<> struct THalfOf<ui16> { typedef ui8 Type; };
template<> struct THalfOf<i32>  { typedef i16 Type; };
template<> struct THalfOf<ui32> { typedef ui16 Type; };
template<> struct THalfOf<i64>  { typedef i32 Type; };
template<> struct THalfOf<ui64> { typedef ui32 Type; };
#ifndef _win_
template<> struct THalfOf<i128_t> { typedef i64 Type; };
template<> struct THalfOf<ui128_t> { typedef ui64 Type; };
#endif
template<typename T> struct THalfOf {};

template<typename T> struct TPairOf;
template<> struct TPairOf<i8>   { typedef i16 Type; };
template<> struct TPairOf<ui8>  { typedef ui16 Type; };
template<> struct TPairOf<i16>  { typedef i32 Type; };
template<> struct TPairOf<ui16> { typedef ui32 Type; };
template<> struct TPairOf<i32>  { typedef i64 Type; };
template<> struct TPairOf<ui32> { typedef ui64 Type; };
#ifndef _win_
template<> struct TPairOf<i64>  { typedef i128_t Type; };
template<> struct TPairOf<ui64> { typedef ui128_t Type; };
#endif
template<typename T> struct TPairOf {};

}

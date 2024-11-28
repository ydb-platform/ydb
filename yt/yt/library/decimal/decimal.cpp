#include "decimal.h"

#include <yt/yt/core/misc/error.h>

#include <util/generic/ylimits.h>
#include <util/string/hex.h>
#include <util/system/byteorder.h>

#include <type_traits>

namespace NYT::NDecimal {

////////////////////////////////////////////////////////////////////////////////

// We use the same type used for binary representation of 256 bit decimals for
// implementing the necessary arithmetic operations.
using i256 = TDecimal::TValue256;

// We chose to only implement a small subset of operations and functions necessary
// for converting 256 bit decimals to and from text.
// Rationale: there do not seem to be any good implementations of 256-bit arithmetic
// that we are comfortable depending on (for now). There are some big number
// implementations around openssl, but these types are intended for arbitrarily
// large integers and use dynamic allocations. It also does not make sense to commit
// to implementing full-fledged 256-bit arithmetics when we actually use a minority
// of operations.
// When we have a good enough int256 either in library/util or in contrib, we can
// switch to it easily.

////////////////////////////////////////////////////////////////////////////////

constexpr i256 operator-(i256 value) noexcept
{
    // Invert.
    for (int partIndex = 0; partIndex < std::ssize(value.Parts); ++partIndex) {
        value.Parts[partIndex] = ~value.Parts[partIndex];
    }

    // Add 1.
    for (int partIndex = 0; partIndex < std::ssize(value.Parts) && ++value.Parts[partIndex] == 0; ++partIndex) { }

    return value;
}

constexpr std::strong_ordering operator<=>(const i256& lhs, const i256& rhs)
{
    bool lhsIsNegative = lhs.Parts.back() & (1u << 31);
    bool rhsIsNegative = rhs.Parts.back() & (1u << 31);

    if (lhsIsNegative && !rhsIsNegative) {
        return std::strong_ordering::less;
    }

    if (!lhsIsNegative && rhsIsNegative) {
        return std::strong_ordering::greater;
    }

    for (int partIndex = std::ssize(lhs.Parts) - 1; partIndex >= 0; --partIndex) {
        if (lhs.Parts[partIndex] != rhs.Parts[partIndex]) {
            return lhs.Parts[partIndex] <=> rhs.Parts[partIndex];
        }
    }

    return std::strong_ordering::equal;
}

// Not synthesized by default :(
constexpr bool operator==(const i256& lhs, const i256& rhs)
{
    return lhs.Parts == rhs.Parts;
}

////////////////////////////////////////////////////////////////////////////////

// Some operations require working with an explicitly unsigned type, since they
// might cause integer overflow, which is not very acceptable for signed types.
// It is also easier to implement some arithmetic operations (like *= 10) by
// shifting bits.
struct TUnsignedValue256
{
    std::array<ui32, 8> Parts;
};

using ui256 = TUnsignedValue256;

static_assert(sizeof(ui256) == sizeof(i256));

////////////////////////////////////////////////////////////////////////////////

constexpr bool operator==(const ui256& lhs, const ui256& rhs)
{
    return lhs.Parts == rhs.Parts;
}

constexpr ui256 operator+(ui256 lhs, const ui256& rhs)
{
    ui64 carry = 0;
    for (int partIndex = 0; partIndex < std::ssize(lhs.Parts); ++partIndex) {
        carry += lhs.Parts[partIndex];
        carry += rhs.Parts[partIndex];
        lhs.Parts[partIndex] = carry;
        carry >>= 32;
    }

    return lhs;
}

template <int Shift>
Y_FORCE_INLINE constexpr ui256 ShiftUp(ui256 value)
{
    static_assert(Shift >= 0 && Shift <= 32);

    value.Parts.back() <<= Shift;
    for (int partIndex = std::ssize(value.Parts) - 2; partIndex >= 0; --partIndex) {
        value.Parts[partIndex + 1] |= value.Parts[partIndex] >> (32 - Shift);
        value.Parts[partIndex] <<= Shift;
    }

    return value;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
constexpr bool ValidDecimalUnderlyingInteger =
    std::is_same_v<T, i32> ||
    std::is_same_v<T, i64> ||
    std::is_same_v<T, i128> ||
    std::is_same_v<T, i256>;

template <typename T>
constexpr bool ValidDecimalUnderlyingUnsignedInteger =
    std::is_same_v<T, ui32> ||
    std::is_same_v<T, ui64> ||
    std::is_same_v<T, ui128> ||
    std::is_same_v<T, ui256>;

template <typename T>
Y_FORCE_INLINE constexpr T GetNan()
{
    if constexpr (std::is_same_v<T, i256>) {
        constexpr i32 i32Max = std::numeric_limits<i32>::max();
        constexpr ui32 ui32Max = std::numeric_limits<ui32>::max();

        return {ui32Max, ui32Max, ui32Max, ui32Max, ui32Max, ui32Max, ui32Max, i32Max};
    } else {
        return std::numeric_limits<T>::max();
    }
}

template <typename T>
Y_FORCE_INLINE constexpr T GetPlusInf()
{
    if constexpr (std::is_same_v<T, i256>) {
        constexpr i32 i32Max = std::numeric_limits<i32>::max();
        constexpr ui32 ui32Max = std::numeric_limits<ui32>::max();

        return {ui32Max - 1, ui32Max, ui32Max, ui32Max, ui32Max, ui32Max, ui32Max, i32Max};
    } else {
        return std::numeric_limits<T>::max() - 1;
    }
}

template <typename T>
struct TDecimalTraits
{
    static_assert(ValidDecimalUnderlyingInteger<T>);

    static constexpr T Nan = GetNan<T>();
    static constexpr T PlusInf = GetPlusInf<T>();
    static constexpr T MinusInf = -PlusInf;
};

template <typename T>
Y_FORCE_INLINE constexpr bool IsNegativeInteger(T value)
{
    static_assert(ValidDecimalUnderlyingInteger<T>);

    if constexpr (std::is_same_v<T, i256>) {
        return value.Parts.back() & (1u << 31);
    } else {
        return value < 0;
    }
}

template <typename T>
Y_FORCE_INLINE constexpr auto DecimalIntegerToUnsigned(T value)
{
    static_assert(ValidDecimalUnderlyingInteger<T>);

    if constexpr (std::is_same_v<T, i256>) {
        return ui256{value.Parts};
    } else if constexpr (std::is_same_v<T, i128>) {
        return ui128(value);
    } else {
        using TU = std::make_unsigned_t<T>;
        return static_cast<TU>(value);
    }
}

template <typename T>
Y_FORCE_INLINE constexpr auto DecimalIntegerToSigned(T value)
{
    static_assert(ValidDecimalUnderlyingUnsignedInteger<T>);

    if constexpr (std::is_same_v<T, ui256>) {
        return i256{value.Parts};
    } else if constexpr (std::is_same_v<T, ui128>) {
        return i128(value);
    } else {
        using TS = std::make_signed_t<T>;
        return static_cast<TS>(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

//! The functions below are used for implementing conversion to/from text to binary decimal.
//! We actually do not need any arithmetic operations beside v *= 10, v /= 10, addition and negation.
//! so they are the only ones actually implemented for our custom i256/ui256.

template <typename T>
Y_FORCE_INLINE constexpr auto FlipMSB(T value)
{
    static_assert(ValidDecimalUnderlyingInteger<T>);

    if constexpr (std::is_same_v<T, i256>) {
        value.Parts.back() ^= (1u << 31);
        return value;
    } else {
        constexpr auto One = DecimalIntegerToUnsigned(T{1});
        // Bit operations are only valid with unsigned types.
        return T(DecimalIntegerToUnsigned(value) ^ (One << (sizeof(T) * 8 - 1)));
    }
}

template <typename T>
Y_FORCE_INLINE constexpr ui32 GetNextDigit(T value, T* nextValue)
{
    static_assert(ValidDecimalUnderlyingUnsignedInteger<T>);

    if constexpr (std::is_same_v<T, ui256>) {
        ui64 remainder = 0;
        for (int partIndex = std::ssize(value.Parts) - 1; partIndex >= 0; --partIndex) {
            // Everything should fit into long long since we are dividing by 10.
            auto step = std::lldiv(value.Parts[partIndex] + (remainder << 32), 10);
            value.Parts[partIndex] = step.quot;
            remainder = step.rem;
        }
        *nextValue = value;
        return remainder;
    } else {
        constexpr auto Ten = T{10};
        *nextValue = value / Ten;
        return static_cast<ui32>(value % Ten);
    }
}

template <typename T>
Y_FORCE_INLINE constexpr T MultiplyByTen(T value)
{
    static_assert(ValidDecimalUnderlyingUnsignedInteger<T>);

    if constexpr (std::is_same_v<T, ui256>) {
        // 2 * (4 * v + v) = 10v.
        return ShiftUp<1>(ShiftUp<2>(value) + value);
    } else {
        return value * DecimalIntegerToUnsigned(10);
    }
}

////////////////////////////////////////////////////////////////////////////////

constexpr int GetDecimalBinaryValueSize(int precision)
{
    if (precision > 0) {
        if (precision <= 9) {
            return 4;
        } else if (precision <= 18) {
            return 8;
        } else if (precision <= 38) {
            return 16;
        } else if (precision <= 76) {
            return 32;
        }
    }
    return 0;
}

static constexpr i32 Decimal32IntegerMaxValueTable[] = {
    0,  // 0
    9,  // 1
    99,  // 2
    999,  // 3
    9999,  // 4
    99999,  // 5
    999999,  // 6
    9999999,  // 7
    99999999,  // 8
    999999999,  // 9
};

static constexpr i64 Decimal64IntegerMaxValueTable[] = {
    9999999999ul,  // 10
    99999999999ul,  // 11
    999999999999ul,  // 12
    9999999999999ul,  // 13
    99999999999999ul,  // 14
    999999999999999ul,  // 15
    9999999999999999ul,  // 16
    99999999999999999ul,  // 17
    999999999999999999ul,  // 18
};

static constexpr i128 Decimal128IntegerMaxValueTable[] = {
    // 128 bits
    //
    // Generated by fair Python script:
    //
    // def print_max_decimal(precision):
    //     max_value = int("9" * precision)
    //     hex_value = hex(max_value)[2:]  # strip 0x
    //     hex_value = hex_value.strip("L")
    //     print("i128{{static_cast<ui64>(0x{}ul)}} | (i128{{static_cast<ui64>(0x{}ul)}} << 64), // {}".format(
    //         hex_value[-16:],
    //         hex_value[:-16] or "0",
    //         precision))
    // for i in range(19, 39):
    //     print_max_decimal(i)
    //
    i128{static_cast<ui64>(0x8ac7230489e7fffful)} | (i128{static_cast<ui64>(0x0ul)} << 64), // 19
    i128{static_cast<ui64>(0x6bc75e2d630ffffful)} | (i128{static_cast<ui64>(0x5ul)} << 64), // 20
    i128{static_cast<ui64>(0x35c9adc5de9ffffful)} | (i128{static_cast<ui64>(0x36ul)} << 64), // 21
    i128{static_cast<ui64>(0x19e0c9bab23ffffful)} | (i128{static_cast<ui64>(0x21eul)} << 64), // 22
    i128{static_cast<ui64>(0x02c7e14af67ffffful)} | (i128{static_cast<ui64>(0x152dul)} << 64), // 23
    i128{static_cast<ui64>(0x1bcecceda0fffffful)} | (i128{static_cast<ui64>(0xd3c2ul)} << 64), // 24
    i128{static_cast<ui64>(0x1614014849fffffful)} | (i128{static_cast<ui64>(0x84595ul)} << 64), // 25
    i128{static_cast<ui64>(0xdcc80cd2e3fffffful)} | (i128{static_cast<ui64>(0x52b7d2ul)} << 64), // 26
    i128{static_cast<ui64>(0x9fd0803ce7fffffful)} | (i128{static_cast<ui64>(0x33b2e3cul)} << 64), // 27
    i128{static_cast<ui64>(0x3e2502610ffffffful)} | (i128{static_cast<ui64>(0x204fce5eul)} << 64), // 28
    i128{static_cast<ui64>(0x6d7217ca9ffffffful)} | (i128{static_cast<ui64>(0x1431e0faeul)} << 64), // 29
    i128{static_cast<ui64>(0x4674edea3ffffffful)} | (i128{static_cast<ui64>(0xc9f2c9cd0ul)} << 64), // 30
    i128{static_cast<ui64>(0xc0914b267ffffffful)} | (i128{static_cast<ui64>(0x7e37be2022ul)} << 64), // 31
    i128{static_cast<ui64>(0x85acef80fffffffful)} | (i128{static_cast<ui64>(0x4ee2d6d415bul)} << 64), // 32
    i128{static_cast<ui64>(0x38c15b09fffffffful)} | (i128{static_cast<ui64>(0x314dc6448d93ul)} << 64), // 33
    i128{static_cast<ui64>(0x378d8e63fffffffful)} | (i128{static_cast<ui64>(0x1ed09bead87c0ul)} << 64), // 34
    i128{static_cast<ui64>(0x2b878fe7fffffffful)} | (i128{static_cast<ui64>(0x13426172c74d82ul)} << 64), // 35
    i128{static_cast<ui64>(0xb34b9f0ffffffffful)} | (i128{static_cast<ui64>(0xc097ce7bc90715ul)} << 64), // 36
    i128{static_cast<ui64>(0x00f4369ffffffffful)} | (i128{static_cast<ui64>(0x785ee10d5da46d9ul)} << 64), // 37
    i128{static_cast<ui64>(0x098a223ffffffffful)} | (i128{static_cast<ui64>(0x4b3b4ca85a86c47aul)} << 64), // 38
};

static constexpr i256 Decimal256IntegerMaxValueTable[] = {
    // 256 bits
    //
    // Generated by fair Python script:
    //
    // def print_max_decimal(precision):
    //     max_value = int("9" * precision)
    //     hex_value = hex(max_value)[2:]  # strip 0x
    //     hex_value = hex_value.strip("L")
    //     parts = [hex_value[-8 * i:-8 * (i - 1) if i > 1 else len(hex_value)] or "0" for i in range(1, 9)]
    //     assert sum(int(v, 16) * 2 ** (32 * i) for i, v in enumerate(parts)) == max_value
    //     assert int(parts[-1], 16) < 2 ** 31 - 1
    //     joined_parts = ", ".join(f"static_cast<ui32>(0x{part}u)" for part in parts)
    //     print(f"{{{joined_parts}}}, // {precision}")
    //
    // for i in range(39, 77):
    //     print_max_decimal(i)
    //
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x5f65567fu), static_cast<ui32>(0x8943acc4u), static_cast<ui32>(0xf050fe93u), static_cast<ui32>(0x2u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 39
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xb9f560ffu), static_cast<ui32>(0x5ca4bfabu), static_cast<ui32>(0x6329f1c3u), static_cast<ui32>(0x1du), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 40
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x4395c9ffu), static_cast<ui32>(0x9e6f7cb5u), static_cast<ui32>(0xdfa371a1u), static_cast<ui32>(0x125u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 41
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xa3d9e3ffu), static_cast<ui32>(0x305adf14u), static_cast<ui32>(0xbc627050u), static_cast<ui32>(0xb7au), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 42
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x6682e7ffu), static_cast<ui32>(0xe38cb6ceu), static_cast<ui32>(0x5bd86321u), static_cast<ui32>(0x72cbu), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 43
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x011d0fffu), static_cast<ui32>(0xe37f2410u), static_cast<ui32>(0x9673df52u), static_cast<ui32>(0x47bf1u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 44
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x0b229fffu), static_cast<ui32>(0xe2f768a0u), static_cast<ui32>(0xe086b93cu), static_cast<ui32>(0x2cd76fu), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 45
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x6f5a3fffu), static_cast<ui32>(0xddaa1640u), static_cast<ui32>(0xc5433c60u), static_cast<ui32>(0x1c06a5eu), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 46
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x59867fffu), static_cast<ui32>(0xa8a4de84u), static_cast<ui32>(0xb4a05bc8u), static_cast<ui32>(0x118427b3u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 47
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x7f40ffffu), static_cast<ui32>(0x9670b12bu), static_cast<ui32>(0x0e4395d6u), static_cast<ui32>(0xaf298d05u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 48
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xf889ffffu), static_cast<ui32>(0xe066ebb2u), static_cast<ui32>(0x8ea3da61u), static_cast<ui32>(0xd79f8232u), static_cast<ui32>(0x6u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 49
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xb563ffffu), static_cast<ui32>(0xc40534fdu), static_cast<ui32>(0x926687d2u), static_cast<ui32>(0x6c3b15f9u), static_cast<ui32>(0x44u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 50
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x15e7ffffu), static_cast<ui32>(0xa83411e9u), static_cast<ui32>(0xb8014e3bu), static_cast<ui32>(0x3a4edbbfu), static_cast<ui32>(0x2acu), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 51
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xdb0fffffu), static_cast<ui32>(0x9208b31au), static_cast<ui32>(0x300d0e54u), static_cast<ui32>(0x4714957du), static_cast<ui32>(0x1abau), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 52
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x8e9fffffu), static_cast<ui32>(0xb456ff0cu), static_cast<ui32>(0xe0828f4du), static_cast<ui32>(0xc6cdd6e3u), static_cast<ui32>(0x10b46u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 53
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x923fffffu), static_cast<ui32>(0x0b65f67du), static_cast<ui32>(0xc5199909u), static_cast<ui32>(0xc40a64e6u), static_cast<ui32>(0xa70c3u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 54
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xb67fffffu), static_cast<ui32>(0x71fba0e7u), static_cast<ui32>(0xb2fffa5au), static_cast<ui32>(0xa867f103u), static_cast<ui32>(0x6867a5u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 55
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x20ffffffu), static_cast<ui32>(0x73d4490du), static_cast<ui32>(0xfdffc788u), static_cast<ui32>(0x940f6a24u), static_cast<ui32>(0x4140c78u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 56
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x49ffffffu), static_cast<ui32>(0x864ada83u), static_cast<ui32>(0xebfdcb54u), static_cast<ui32>(0xc89a2571u), static_cast<ui32>(0x28c87cb5u), static_cast<ui32>(0x0u), static_cast<ui32>(0x0u)}, // 57
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xe3ffffffu), static_cast<ui32>(0x3eec8920u), static_cast<ui32>(0x37e9f14du), static_cast<ui32>(0xd6057673u), static_cast<ui32>(0x97d4df19u), static_cast<ui32>(0x1u), static_cast<ui32>(0x0u)}, // 58
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xe7ffffffu), static_cast<ui32>(0x753d5b48u), static_cast<ui32>(0x2f236d04u), static_cast<ui32>(0x5c36a080u), static_cast<ui32>(0xee50b702u), static_cast<ui32>(0xfu), static_cast<ui32>(0x0u)}, // 59
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x0fffffffu), static_cast<ui32>(0x946590d9u), static_cast<ui32>(0xd762422cu), static_cast<ui32>(0x9a224501u), static_cast<ui32>(0x4f272617u), static_cast<ui32>(0x9fu), static_cast<ui32>(0x0u)}, // 60
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x9fffffffu), static_cast<ui32>(0xcbf7a87au), static_cast<ui32>(0x69d695bdu), static_cast<ui32>(0x0556b212u), static_cast<ui32>(0x17877cecu), static_cast<ui32>(0x639u), static_cast<ui32>(0x0u)}, // 61
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x3fffffffu), static_cast<ui32>(0xf7ac94cau), static_cast<ui32>(0x2261d969u), static_cast<ui32>(0x3562f4b8u), static_cast<ui32>(0xeb4ae138u), static_cast<ui32>(0x3e3au), static_cast<ui32>(0x0u)}, // 62
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x7fffffffu), static_cast<ui32>(0xacbdcfe6u), static_cast<ui32>(0x57d27e23u), static_cast<ui32>(0x15dd8f31u), static_cast<ui32>(0x30eccc32u), static_cast<ui32>(0x26e4du), static_cast<ui32>(0x0u)}, // 63
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xbf6a1f00u), static_cast<ui32>(0x6e38ed64u), static_cast<ui32>(0xdaa797edu), static_cast<ui32>(0xe93ff9f4u), static_cast<ui32>(0x184f03u), static_cast<ui32>(0x0u)}, // 64
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x7a253609u), static_cast<ui32>(0x4e3945efu), static_cast<ui32>(0x8a8bef46u), static_cast<ui32>(0x1c7fc390u), static_cast<ui32>(0xf31627u), static_cast<ui32>(0x0u)}, // 65
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xc5741c63u), static_cast<ui32>(0x0e3cbb5au), static_cast<ui32>(0x697758bfu), static_cast<ui32>(0x1cfda3a5u), static_cast<ui32>(0x97edd87u), static_cast<ui32>(0x0u)}, // 66
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xb6891be7u), static_cast<ui32>(0x8e5f518bu), static_cast<ui32>(0x1ea97776u), static_cast<ui32>(0x21e86476u), static_cast<ui32>(0x5ef4a747u), static_cast<ui32>(0x0u)}, // 67
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x215b170fu), static_cast<ui32>(0x8fb92f75u), static_cast<ui32>(0x329eaaa1u), static_cast<ui32>(0x5313ec9du), static_cast<ui32>(0xb58e88c7u), static_cast<ui32>(0x3u)}, // 68
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x4d8ee69fu), static_cast<ui32>(0x9d3bda93u), static_cast<ui32>(0xfa32aa4fu), static_cast<ui32>(0x3ec73e23u), static_cast<ui32>(0x179157c9u), static_cast<ui32>(0x25u)}, // 69
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x0795023fu), static_cast<ui32>(0x245689c1u), static_cast<ui32>(0xc5faa71cu), static_cast<ui32>(0x73c86d67u), static_cast<ui32>(0xebad6ddcu), static_cast<ui32>(0x172u)}, // 70
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x4bd2167fu), static_cast<ui32>(0x6b61618au), static_cast<ui32>(0xbbca8719u), static_cast<ui32>(0x85d4460du), static_cast<ui32>(0x34c64a9cu), static_cast<ui32>(0xe7du)}, // 71
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xf634e0ffu), static_cast<ui32>(0x31cdcf66u), static_cast<ui32>(0x55e946feu), static_cast<ui32>(0x3a4abc89u), static_cast<ui32>(0x0fbeea1du), static_cast<ui32>(0x90e4u)}, // 72
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x9e10c9ffu), static_cast<ui32>(0xf20a1a05u), static_cast<ui32>(0x5b1cc5edu), static_cast<ui32>(0x46eb5d5du), static_cast<ui32>(0x9d752524u), static_cast<ui32>(0x5a8e8u)}, // 73
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x2ca7e3ffu), static_cast<ui32>(0x74650438u), static_cast<ui32>(0x8f1fbb4bu), static_cast<ui32>(0xc531a5a5u), static_cast<ui32>(0x2693736au), static_cast<ui32>(0x389916u)}, // 74
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xbe8ee7ffu), static_cast<ui32>(0x8bf22a31u), static_cast<ui32>(0x973d50f2u), static_cast<ui32>(0xb3f07877u), static_cast<ui32>(0x81c2822bu), static_cast<ui32>(0x235faddu)}, // 75
    {static_cast<ui32>(0xffffffffu), static_cast<ui32>(0xffffffffu), static_cast<ui32>(0x71950fffu), static_cast<ui32>(0x7775a5f1u), static_cast<ui32>(0xe8652979u), static_cast<ui32>(0x0764b4abu), static_cast<ui32>(0x119915b5u), static_cast<ui32>(0x161bcca7u)}, // 76
};

template<typename T>
Y_FORCE_INLINE constexpr auto GetDecimalMaxIntegerValue(int precision)
{
    static_assert(ValidDecimalUnderlyingInteger<T>);

    if (TDecimal::GetValueBinarySize(precision) > static_cast<int>(sizeof(T))) {
        YT_ABORT();
    }

    if constexpr (std::is_same_v<T, i32>) {
        YT_VERIFY(precision <= 9);
        return Decimal32IntegerMaxValueTable[precision];
    } else if constexpr (std::is_same_v<T, i64>) {
        YT_VERIFY(precision >= 10 && precision <= 18);
        return Decimal64IntegerMaxValueTable[precision - 10];
    } else if constexpr (std::is_same_v<T, i128>) {
        YT_VERIFY(precision >= 19 && precision <= 38);
        return Decimal128IntegerMaxValueTable[precision - 19];
    } else if constexpr (std::is_same_v<T, i256>) {
        YT_VERIFY(precision >= 39 && precision <= 76);
        return Decimal256IntegerMaxValueTable[precision - 39];
    } else {
        YT_ABORT();
    }
}

template <typename T>
static Y_FORCE_INLINE T DecimalHostToInet(T value)
{
    if constexpr (std::is_same_v<T, i256> || std::is_same_v<T, ui256>) {
        for (int partIndex = 0; partIndex < std::ssize(value.Parts) / 2; ++partIndex) {
            value.Parts[partIndex] = ::HostToInet(value.Parts[partIndex]);
            value.Parts[std::size(value.Parts) - 1 - partIndex] = ::HostToInet(value.Parts[std::size(value.Parts) - 1 - partIndex]);
            std::swap(value.Parts[partIndex], value.Parts[std::size(value.Parts) - 1 - partIndex]);
        }
        return value;
    } else if constexpr (std::is_same_v<T, i128> || std::is_same_v<T, ui128>) {
        return T(::HostToInet(GetLow(value)), ::HostToInet(GetHigh(value)));
    } else {
        return ::HostToInet(value);
    }
}

template <typename T>
static Y_FORCE_INLINE T DecimalInetToHost(T value)
{
    if constexpr (std::is_same_v<T, i256> || std::is_same_v<T, ui256>) {
        for (int partIndex = 0; partIndex < std::ssize(value.Parts) / 2; ++partIndex) {
            value.Parts[partIndex] = ::InetToHost(value.Parts[partIndex]);
            value.Parts[std::size(value.Parts) - 1 - partIndex] = ::InetToHost(value.Parts[std::size(value.Parts) - 1 - partIndex]);
            std::swap(value.Parts[partIndex], value.Parts[std::size(value.Parts) - 1 - partIndex]);
        }
        return value;
    } else if constexpr (std::is_same_v<T, i128> || std::is_same_v<T, ui128>) {
        return T(::InetToHost(GetLow(value)), ::InetToHost(GetHigh(value)));
    } else {
        return ::InetToHost(value);
    }
}

template <typename T>
static T DecimalBinaryToIntegerUnchecked(TStringBuf binaryValue)
{
    T result;
    memcpy(&result, binaryValue.data(), sizeof(result));
    return FlipMSB(DecimalInetToHost(result));
}

template<typename T>
static void DecimalIntegerToBinaryUnchecked(T decodedValue, void* buf)
{
    auto preparedValue = DecimalHostToInet(FlipMSB(decodedValue));
    memcpy(buf, &preparedValue, sizeof(preparedValue));
}

static void CheckDecimalValueSize(TStringBuf value, int precision, int scale)
{
    int expectedSize = TDecimal::GetValueBinarySize(precision);
    if (std::ssize(value) != expectedSize) {
        THROW_ERROR_EXCEPTION(
            "Decimal<%v,%v> binary value representation has invalid length: actual %v, expected %v",
            precision,
            scale,
            value.size(),
            expectedSize);
    }
}

static Y_FORCE_INLINE TStringBuf PlaceOnBuffer(TStringBuf value, char* buffer)
{
    memcpy(buffer, value.data(), value.size());
    return TStringBuf(buffer, value.size());
}

// TODO(ermolovd): make it FASTER (check NYT::WriteDecIntToBufferBackwards)
template<typename T>
static TStringBuf WriteTextDecimalUnchecked(T decodedValue, int scale, char* buffer)
{
    if (decodedValue == TDecimalTraits<T>::MinusInf) {
        static constexpr TStringBuf minusInf = "-inf";
        return PlaceOnBuffer(minusInf, buffer);
    } else if (decodedValue == TDecimalTraits<T>::PlusInf) {
        static constexpr TStringBuf inf = "inf";
        return PlaceOnBuffer(inf, buffer);
    } else if (decodedValue == TDecimalTraits<T>::Nan) {
        static constexpr TStringBuf nan = "nan";
        return PlaceOnBuffer(nan, buffer);
    }

    i8 digits[TDecimal::MaxTextSize] = {0,};

    bool negative = IsNegativeInteger(decodedValue);
    auto absValue = DecimalIntegerToUnsigned(negative ? -decodedValue : decodedValue);

    auto* curDigit = digits;
    while (absValue != DecimalIntegerToUnsigned(T{0})) {
        *curDigit = GetNextDigit(absValue, &absValue);
        curDigit++;
    }
    YT_VERIFY(curDigit <= digits + std::size(digits));

    if (curDigit - digits <= scale) {
        curDigit = digits + scale + 1;
    }

    char* bufferPosition = buffer;
    if (negative) {
        *bufferPosition = '-';
        ++bufferPosition;
    }
    while (curDigit > digits + scale) {
        --curDigit;
        *bufferPosition = '0' + *curDigit;
        ++bufferPosition;
    }
    if (scale > 0) {
        *bufferPosition = '.';
        ++bufferPosition;
        while (curDigit > digits) {
            --curDigit;
            *bufferPosition = '0' + *curDigit;
            ++bufferPosition;
        }
    }
    return TStringBuf(buffer, bufferPosition - buffer);
}

void ThrowInvalidDecimal(TStringBuf value, int precision, int scale, const char* reason = nullptr)
{
    if (reason == nullptr) {
        THROW_ERROR_EXCEPTION(
            "String %Qv is not valid Decimal<%v,%v> representation",
            value,
            precision,
            scale);
    } else {
        THROW_ERROR_EXCEPTION(
            "String %Qv is not valid Decimal<%v,%v> representation: %v",
            value,
            precision,
            scale,
            reason);
    }
}

template<typename T>
T DecimalTextToInteger(TStringBuf textValue, int precision, int scale)
{
    if (textValue.empty()) {
        ThrowInvalidDecimal(textValue, precision, scale);
    }

    auto cur = textValue.cbegin();
    auto end = textValue.end();

    bool negative = false;
    switch (*cur) {
        case '-':
            negative = true;
            [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
        case '+':
            ++cur;
            break;
    }
    if (cur == end) {
        ThrowInvalidDecimal(textValue, precision, scale);
    }

    switch (*cur) {
        case 'i':
        case 'I':
            if (cur + 3 == end) {
                ++cur;
                if (*cur == 'n' || *cur == 'N') {
                    ++cur;
                    if (*cur == 'f' || *cur == 'F') {
                        return negative ? TDecimalTraits<T>::MinusInf : TDecimalTraits<T>::PlusInf;
                    }
                }
            }
            ThrowInvalidDecimal(textValue, precision, scale);
            [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
        case 'n':
        case 'N':
            if (!negative && cur + 3 == end) {
                ++cur;
                if (*cur == 'a' || *cur == 'A') {
                    ++cur;
                    if (*cur == 'n' || *cur == 'N') {
                        return TDecimalTraits<T>::Nan;
                    }
                }
            }
            ThrowInvalidDecimal(textValue, precision, scale);
            break;
    }

    // This value can overflow in the process of parsing the text value.
    // We do throw an exception later, but UB is not good for your health.
    auto result = DecimalIntegerToUnsigned(T{0});
    int beforePoint = 0;
    int afterPoint = 0;

    auto addDigit = [&] (auto digit) {
        // We use this type to avoid warnings about casting signed types to unsigned/narrowing ints.
        // Ugly, but this way we don't need to define cumbersome constructors for TValue256.
        ui16 currentDigit = *digit - '0';
        if (currentDigit < 0 || currentDigit > 9) {
            ThrowInvalidDecimal(textValue, precision, scale);
        }

        result = MultiplyByTen(result);
        result = result + DecimalIntegerToUnsigned(T{currentDigit});
    };

    for (; cur != end; ++cur) {
        if (*cur == '.') {
            ++cur;
            for (; cur != end; ++cur) {
                addDigit(cur);
                ++afterPoint;
            }
            break;
        }

        addDigit(cur);
        ++beforePoint;
    }

    for (; afterPoint < scale; ++afterPoint) {
        result = MultiplyByTen(result);
    }

    if (afterPoint > scale) {
        ThrowInvalidDecimal(textValue, precision, scale, "too many digits after decimal point");
    }

    if (beforePoint + scale > precision) {
        ThrowInvalidDecimal(textValue, precision, scale, "too many digits before decimal point");
    }

    // This cast is guaranteed by the checks above to be correct.
    auto signedResult = DecimalIntegerToSigned(result);
    // This is safe: the range of representable values fits into [-signed_max, signed_max] for each underlying type.
    return negative ? -signedResult : signedResult;
}

template<typename T>
Y_FORCE_INLINE TStringBuf DecimalBinaryToTextUncheckedImpl(TStringBuf value, int scale, char* buffer)
{
    T decoded = DecimalBinaryToIntegerUnchecked<T>(value);
    return WriteTextDecimalUnchecked(decoded, scale, buffer);
}

TStringBuf TDecimal::BinaryToText(TStringBuf binaryDecimal, int precision, int scale, char* buffer, size_t bufferSize)
{
    ValidatePrecisionAndScale(precision, scale);

    YT_VERIFY(bufferSize >= MaxTextSize);
    switch (binaryDecimal.size()) {
        case 4:
            return DecimalBinaryToTextUncheckedImpl<i32>(binaryDecimal, scale, buffer);
        case 8:
            return DecimalBinaryToTextUncheckedImpl<i64>(binaryDecimal, scale, buffer);
        case 16:
            return DecimalBinaryToTextUncheckedImpl<i128>(binaryDecimal, scale, buffer);
        case 32:
            return DecimalBinaryToTextUncheckedImpl<i256>(binaryDecimal, scale, buffer);
    }
    CheckDecimalValueSize(binaryDecimal, precision, scale);
    YT_ABORT();
}

TString TDecimal::BinaryToText(TStringBuf binaryDecimal, int precision, int scale)
{
    TString result;
    result.ReserveAndResize(MaxTextSize);
    auto resultSize = BinaryToText(binaryDecimal, precision, scale, result.Detach(), result.size()).size();
    result.resize(resultSize);
    return result;
}

template<typename T>
TStringBuf TextToBinaryImpl(TStringBuf textDecimal, int precision, int scale, char* buffer)
{
    T decoded = DecimalTextToInteger<T>(textDecimal, precision, scale);
    DecimalIntegerToBinaryUnchecked(decoded, buffer);
    return TStringBuf(buffer, TDecimal::GetValueBinarySize(precision));
}

TStringBuf TDecimal::TextToBinary(TStringBuf textValue, int precision, int scale, char* buffer, size_t bufferSize)
{
    ValidatePrecisionAndScale(precision, scale);

    YT_VERIFY(bufferSize >= static_cast<size_t>(TDecimal::GetValueBinarySize(precision)));

    int byteSize = TDecimal::GetValueBinarySize(precision);
    switch (byteSize) {
        case 4:
            return TextToBinaryImpl<i32>(textValue, precision, scale, buffer);
        case 8:
            return TextToBinaryImpl<i64>(textValue, precision, scale, buffer);
        case 16:
            return TextToBinaryImpl<i128>(textValue, precision, scale, buffer);
        case 32:
            return TextToBinaryImpl<i256>(textValue, precision, scale, buffer);
        default:
            static_assert(GetDecimalBinaryValueSize(TDecimal::MaxPrecision) == 32);
            YT_ABORT();
    }
}

TString TDecimal::TextToBinary(TStringBuf textValue, int precision, int scale)
{
    TString result;
    result.ReserveAndResize(MaxBinarySize);
    auto resultSize = TextToBinary(textValue, precision, scale, result.Detach(), result.size()).size();
    result.resize(resultSize);
    return result;
}

void TDecimal::ValidatePrecisionAndScale(int precision, int scale)
{
    if (precision <= 0 || precision > MaxPrecision) {
        THROW_ERROR_EXCEPTION("Invalid decimal precision %Qlv, precision must be in range [1, %v]",
            precision,
            MaxPrecision);

    } else if (scale < 0 || scale > precision) {
        THROW_ERROR_EXCEPTION("Invalid decimal scale %v (precision: %v); decimal scale must be in range [0, PRECISION]",
            scale,
            precision);
    }
}

template <typename T>
static void ValidateDecimalBinaryValueImpl(TStringBuf binaryDecimal, int precision, int scale)
{
    T decoded = DecimalBinaryToIntegerUnchecked<T>(binaryDecimal);

    auto maxValue = GetDecimalMaxIntegerValue<T>(precision);

    if (-maxValue <= decoded && decoded <= maxValue) {
        return;
    }

    if (decoded == TDecimalTraits<T>::MinusInf ||
        decoded == TDecimalTraits<T>::PlusInf ||
        decoded == TDecimalTraits<T>::Nan)
    {
        return;
    }

    char textBuffer[TDecimal::MaxTextSize];
    auto textDecimal = WriteTextDecimalUnchecked<T>(decoded, scale, textBuffer);

    THROW_ERROR_EXCEPTION(
        "Decimal<%v,%v> does not have enough precision to represent %Qv",
        precision,
        scale,
        textDecimal)
        << TErrorAttribute("binary_value", HexEncode(binaryDecimal));
}

void TDecimal::ValidateBinaryValue(TStringBuf binaryDecimal, int precision, int scale)
{
    CheckDecimalValueSize(binaryDecimal, precision, scale);
    switch (binaryDecimal.size()) {
        case 4:
            return ValidateDecimalBinaryValueImpl<i32>(binaryDecimal, precision, scale);
        case 8:
            return ValidateDecimalBinaryValueImpl<i64>(binaryDecimal, precision, scale);
        case 16:
            return ValidateDecimalBinaryValueImpl<i128>(binaryDecimal, precision, scale);
        case 32:
            return ValidateDecimalBinaryValueImpl<i256>(binaryDecimal, precision, scale);
        default:
            static_assert(GetDecimalBinaryValueSize(TDecimal::MaxPrecision) == 32);
            YT_ABORT();
    }
}

template <typename T>
Y_FORCE_INLINE void CheckDecimalIntBits(int precision)
{
    const auto expectedSize = TDecimal::GetValueBinarySize(precision);
    if (expectedSize != sizeof(T)) {
        const int bitCount = sizeof(T) * 8;
        THROW_ERROR_EXCEPTION("Decimal<%v, ?> cannot be represented as int%v",
            precision,
            bitCount);
    }
}

Y_FORCE_INLINE void CheckDecimalFitsInto128Bits(int precision)
{
    if (precision > 38) {
        THROW_ERROR_EXCEPTION("Decimal<%v, ?> does not fit into int128",
            precision);
    }
}

int TDecimal::GetValueBinarySize(int precision)
{
    const auto result = GetDecimalBinaryValueSize(precision);
    if (result <= 0) {
        ValidatePrecisionAndScale(precision, 0);
        YT_ABORT();
    }
    return result;
}

TStringBuf TDecimal::WriteBinary32(int precision, i32 value, char* buffer, size_t bufferLength)
{
    const size_t resultLength = GetValueBinarySize(precision);
    CheckDecimalIntBits<i32>(precision);
    YT_VERIFY(bufferLength >= resultLength);

    DecimalIntegerToBinaryUnchecked(value, buffer);
    return TStringBuf{buffer, sizeof(value)};
}

TStringBuf TDecimal::WriteBinary64(int precision, i64 value, char* buffer, size_t bufferLength)
{
    const size_t resultLength = GetValueBinarySize(precision);
    CheckDecimalIntBits<i64>(precision);
    YT_VERIFY(bufferLength >= resultLength);

    DecimalIntegerToBinaryUnchecked(value, buffer);
    return TStringBuf{buffer, sizeof(value)};
}

TStringBuf TDecimal::WriteBinary128(int precision, TValue128 value, char* buffer, size_t bufferLength)
{
    const size_t resultLength = GetValueBinarySize(precision);
    CheckDecimalIntBits<TValue128>(precision);
    YT_VERIFY(bufferLength >= resultLength);

    DecimalIntegerToBinaryUnchecked(i128(value.High, value.Low), buffer);
    return TStringBuf{buffer, sizeof(TValue128)};
}

TStringBuf TDecimal::WriteBinary128Variadic(int precision, TValue128 value, char* buffer, size_t bufferLength)
{
    const size_t resultLength = GetValueBinarySize(precision);
    switch (resultLength) {
        case 4:
            return WriteBinary32(precision, static_cast<i32>(value.Low), buffer, bufferLength);
        case 8:
            return WriteBinary64(precision, static_cast<i64>(value.Low), buffer, bufferLength);
        case 16:
            return WriteBinary128(precision, value, buffer, bufferLength);
        default:
            THROW_ERROR_EXCEPTION("Invalid precision %v", precision);
    }
}

TStringBuf TDecimal::WriteBinary256(int precision, TValue256 value, char* buffer, size_t bufferLength)
{
    const size_t resultLength = GetValueBinarySize(precision);
    CheckDecimalIntBits<TValue256>(precision);
    YT_VERIFY(bufferLength >= resultLength);

    DecimalIntegerToBinaryUnchecked(value, buffer);
    return TStringBuf{buffer, sizeof(TValue256)};
}

TStringBuf TDecimal::WriteBinary256Variadic(int precision, TValue256 value, char* buffer, size_t bufferLength)
{
    const size_t resultLength = GetValueBinarySize(precision);
    switch (resultLength) {
        case 4:
            return WriteBinary32(precision, *reinterpret_cast<i32*>(value.Parts.data()), buffer, bufferLength);
        case 8:
            return WriteBinary64(precision, *reinterpret_cast<i64*>(value.Parts.data()), buffer, bufferLength);
        case 16:
            return WriteBinary128(precision, *reinterpret_cast<TValue128*>(value.Parts.data()), buffer, bufferLength);
        case 32:
            return WriteBinary256(precision, value, buffer, bufferLength);
        default:
            THROW_ERROR_EXCEPTION("Invalid precision %v", precision);
    }
}


template <typename T>
Y_FORCE_INLINE void CheckBufferLength(int precision, size_t bufferLength)
{
    CheckDecimalIntBits<T>(precision);
    if (sizeof(T) != bufferLength) {
        THROW_ERROR_EXCEPTION("Decimal<%v, ?> has unexpected length: expected %v, actual %v",
            precision,
            sizeof(T),
            bufferLength);
    }
}

i32 TDecimal::ParseBinary32(int precision, TStringBuf buffer)
{
    CheckBufferLength<i32>(precision, buffer.size());
    return DecimalBinaryToIntegerUnchecked<i32>(buffer);
}

i64 TDecimal::ParseBinary64(int precision, TStringBuf buffer)
{
    CheckBufferLength<i64>(precision, buffer.size());
    return DecimalBinaryToIntegerUnchecked<i64>(buffer);
}

TDecimal::TValue128 TDecimal::ParseBinary128(int precision, TStringBuf buffer)
{
    CheckBufferLength<i128>(precision, buffer.size());
    auto result = DecimalBinaryToIntegerUnchecked<i128>(buffer);
    return {GetLow(result), static_cast<i64>(GetHigh(result))};
}

TDecimal::TValue256 TDecimal::ParseBinary256(int precision, TStringBuf buffer)
{
    CheckBufferLength<i256>(precision, buffer.Size());
    return DecimalBinaryToIntegerUnchecked<i256>(buffer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDecimal

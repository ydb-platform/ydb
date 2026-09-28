#pragma once

#include <util/system/types.h>

#include <bit>
#include <type_traits>

// IEEE-754 binary16 stored in native endian.
struct TFloat16 {
    ui16 Bits = 0;

    TFloat16() = default;
    TFloat16(const TFloat16&) = default;
    TFloat16& operator=(const TFloat16&) = default;

    explicit constexpr TFloat16(float value) noexcept
        : Bits(FloatToBits(value))
    {
    }

    explicit constexpr operator float() const noexcept {
        return BitsToFloat(Bits);
    }

private:
    static constexpr ui16 FloatToBits(float value) noexcept {
        const ui32 f = std::bit_cast<ui32>(value);
        const ui16 sign = static_cast<ui16>((f >> 16) & 0x8000u);
        const i32 exp = static_cast<i32>((f >> 23) & 0xff);
        ui32 mantissa = f & 0x007fffffu;
        const i32 e = exp - 112; // 127 - 15

        if (e <= 0) {
            if (e < -10) {
                return sign;
            }
            mantissa |= 0x00800000u;
            const ui32 shift = static_cast<ui32>(14 - e);
            const ui32 halfMinusLsb = (1u << (shift - 1)) - 1u;
            const ui32 lsb = (mantissa >> shift) & 1u;
            mantissa = (mantissa + halfMinusLsb + lsb) >> shift;
            return static_cast<ui16>(sign | mantissa);
        }

        if (exp == 0xff) {
            if (mantissa == 0) {
                return static_cast<ui16>(sign | 0x7c00u);
            }
            const ui16 nanPayload = static_cast<ui16>(mantissa >> 13);
            return static_cast<ui16>(sign | 0x7e00u | nanPayload);
        }

        mantissa += 0x00000fffu + ((mantissa >> 13) & 1u);
        i32 outExp = e;
        if (mantissa & 0x00800000u) {
            mantissa = 0;
            ++outExp;
        }
        if (outExp >= 31) {
            return static_cast<ui16>(sign | 0x7c00u);
        }
        return static_cast<ui16>(sign | (static_cast<ui32>(outExp) << 10) | (mantissa >> 13));
    }

    static constexpr float BitsToFloat(ui16 bits) noexcept {
        const ui32 sign = (static_cast<ui32>(bits) & 0x8000u) << 16;
        i32 exp = (bits >> 10) & 0x1f;
        ui32 mantissa = bits & 0x03ffu;

        if (exp == 0) {
            if (mantissa == 0) {
                return std::bit_cast<float>(sign);
            }
            exp = 1;
            while ((mantissa & 0x0400u) == 0) {
                mantissa <<= 1;
                --exp;
            }
            mantissa &= 0x03ffu;
            const ui32 f32 = sign | (static_cast<ui32>(exp + 127 - 15) << 23) | (mantissa << 13);
            return std::bit_cast<float>(f32);
        }
        if (exp == 0x1f) {
            const ui32 f32 = sign | 0x7f800000u | (mantissa << 13);
            return std::bit_cast<float>(f32);
        }
        const ui32 f32 = sign | (static_cast<ui32>(exp + 127 - 15) << 23) | (mantissa << 13);
        return std::bit_cast<float>(f32);
    }
};

static_assert(sizeof(TFloat16) == 2);
static_assert(std::is_trivially_copyable_v<TFloat16>);
static_assert(TFloat16(1.0f).Bits == 0x3c00);
static_assert(TFloat16(0.5f).Bits == 0x3800);
static_assert(TFloat16(-1.0f).Bits == 0xbc00);
static_assert(static_cast<float>(TFloat16(1.0f)) == 1.0f);
static_assert(static_cast<float>(TFloat16(0.5f)) == 0.5f);

// bfloat16: float32 sign+exponent plus the top 7 mantissa bits, native endian.
struct TBFloat16 {
    ui16 Bits = 0;

    TBFloat16() = default;
    TBFloat16(const TBFloat16&) = default;
    TBFloat16& operator=(const TBFloat16&) = default;

    explicit constexpr TBFloat16(float value) noexcept
        : Bits(FloatToBits(value))
    {
    }

    explicit constexpr operator float() const noexcept {
        return BitsToFloat(Bits);
    }

private:
    static constexpr ui16 FloatToBits(float value) noexcept {
        ui32 f = std::bit_cast<ui32>(value);
        if ((f & 0x7fffffffu) > 0x7f800000u) {
            return static_cast<ui16>((f >> 16) | 0x0040u);
        }
        // round-to-nearest-even
        const ui32 lsb = (f >> 16) & 1u;
        f += 0x7fffu + lsb;
        return static_cast<ui16>(f >> 16);
    }

    static constexpr float BitsToFloat(ui16 bits) noexcept {
        return std::bit_cast<float>(static_cast<ui32>(bits) << 16);
    }
};

static_assert(sizeof(TBFloat16) == 2);
static_assert(std::is_trivially_copyable_v<TBFloat16>);
static_assert(TBFloat16(1.0f).Bits == 0x3f80);
static_assert(TBFloat16(1.003f).Bits == 0x3f80);
static_assert(TBFloat16(1.004f).Bits == 0x3f81);
static_assert(static_cast<float>(TBFloat16(1.004f)) == 1.0078125f);

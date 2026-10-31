#pragma once

#include <util/system/types.h>

#include <bit>
#include <type_traits>

namespace NVectorDistance::NPrivate {

    // Unscaled FP8. Conversion from float uses round-to-nearest, ties-to-even,
    // saturating overflow and infinities to the signed maximum finite value.
    // NaNs are canonicalized; signed zero and subnormals are preserved.
    template <unsigned MantissaBits_, unsigned ExponentBias_, bool FiniteOnly_>
    struct TFloat8 {
        static constexpr unsigned MantissaBits = MantissaBits_;
        static constexpr unsigned ExponentBias = ExponentBias_;
        static constexpr bool FiniteOnly = FiniteOnly_;
        static constexpr ui8 MaxFiniteBits = FiniteOnly ? 0x7e : 0x7b;
        static constexpr ui8 NaNBits = FiniteOnly ? 0x7f : 0x7e;

        ui8 Bits = 0;

        TFloat8() = default;

        explicit constexpr TFloat8(float value) noexcept
            : Bits(FloatToBits(value))
        {
        }

        static constexpr TFloat8 FromBits(ui8 bits) noexcept {
            TFloat8 result;
            result.Bits = bits;
            return result;
        }

        explicit constexpr operator float() const noexcept {
            const ui32 sign = static_cast<ui32>(Bits & 0x80u) << 24;
            const ui32 magnitude = Bits & 0x7fu;
            i32 exponent = magnitude >> MantissaBits;
            ui32 mantissa = magnitude & ((1u << MantissaBits) - 1u);

            if constexpr (FiniteOnly) {
                if (magnitude == 0x7f) {
                    return std::bit_cast<float>(sign | 0x7fc00000u);
                }
            } else {
                if (exponent == 0x1f) {
                    return std::bit_cast<float>(sign | 0x7f800000u | (mantissa << (23 - MantissaBits)));
                }
            }

            if (exponent == 0) {
                if (mantissa == 0) {
                    return std::bit_cast<float>(sign);
                }
                exponent = 1;
                while ((mantissa & (1u << MantissaBits)) == 0) {
                    mantissa <<= 1;
                    --exponent;
                }
                mantissa &= (1u << MantissaBits) - 1u;
            }

            return std::bit_cast<float>(sign
                | (static_cast<ui32>(exponent + 127 - ExponentBias) << 23)
                | (mantissa << (23 - MantissaBits)));
        }

    private:
        static constexpr ui8 FloatToBits(float value) noexcept {
            const ui32 bits = std::bit_cast<ui32>(value);
            const ui8 sign = (bits >> 24) & 0x80u;
            const ui32 exponent = (bits >> 23) & 0xffu;
            ui32 mantissa = bits & 0x007fffffu;

            if (exponent == 0xff) {
                return sign | (mantissa != 0 ? NaNBits : MaxFiniteBits);
            }

            const i32 targetExponent = static_cast<i32>(exponent) - 127 + static_cast<i32>(ExponentBias);
            if (targetExponent <= 0) {
                if (targetExponent < -static_cast<i32>(MantissaBits)) {
                    return sign;
                }
                mantissa |= 0x00800000u;
                const unsigned shift = 24 - static_cast<i32>(MantissaBits) - targetExponent;
                const ui32 rounded = (mantissa + ((1u << (shift - 1)) - 1u)
                    + ((mantissa >> shift) & 1u)) >> shift;
                return sign | static_cast<ui8>(rounded);
            }

            constexpr unsigned shift = 23 - MantissaBits;
            const ui32 rounded = (mantissa + ((1u << (shift - 1)) - 1u)
                + ((mantissa >> shift) & 1u)) >> shift;
            const ui32 magnitude = (static_cast<ui32>(targetExponent) << MantissaBits) + rounded;
            return sign | static_cast<ui8>(magnitude > MaxFiniteBits ? MaxFiniteBits : magnitude);
        }
    };

} // namespace NVectorDistance::NPrivate

// E4M3 means E4M3FN: bias 7, maximum 448, NaN but no infinity.
// These types do not use the incompatible FNUZ encodings.
using TFloat8E4M3 = NVectorDistance::NPrivate::TFloat8<3, 7, true>;
using TFloat8E5M2 = NVectorDistance::NPrivate::TFloat8<2, 15, false>;

static_assert(sizeof(TFloat8E4M3) == 1 && std::is_trivially_copyable_v<TFloat8E4M3>);
static_assert(sizeof(TFloat8E5M2) == 1 && std::is_trivially_copyable_v<TFloat8E5M2>);
static_assert(TFloat8E4M3(1.0f).Bits == 0x38);
static_assert(TFloat8E5M2(1.0f).Bits == 0x3c);
static_assert(static_cast<float>(TFloat8E4M3::FromBits(0x7e)) == 448.0f);
static_assert(static_cast<float>(TFloat8E5M2::FromBits(0x7b)) == 57344.0f);

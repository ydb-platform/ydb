#pragma once

#include <util/system/types.h>

#include <limits>
#include <type_traits>

namespace NVectorDistance::NPrivate {

    // A logical coordinate. Serialized vectors pack the first coordinate in the
    // low nibble and the second in the high nibble; signed values use two's complement.
    template <bool Signed>
    struct TInt4 {
        static constexpr int MinValue = Signed ? -8 : 0;
        static constexpr int MaxValue = Signed ? 7 : 15;
        ui8 Bits = 0;

        constexpr TInt4() = default;
        template <typename T> requires std::is_arithmetic_v<T>
        explicit constexpr TInt4(T value) noexcept {
            // Clamp before converting to an integer, including infinities. NaN
            // maps to zero; finite fractional values truncate toward zero.
            const double numeric = static_cast<double>(value);
            const int clamped = numeric != numeric ? 0 : numeric <= MinValue ? MinValue
                : numeric >= MaxValue ? MaxValue : static_cast<int>(numeric);
            Bits = static_cast<ui8>(clamped) & 15;
        }

        static constexpr TInt4 FromBits(ui8 bits) noexcept {
            TInt4 result;
            result.Bits = bits & 15;
            return result;
        }

        constexpr operator int() const noexcept {
            return Signed && (Bits & 8) ? static_cast<int>(Bits) - 16 : Bits;
        }
    };
}

using TUint4 = NVectorDistance::NPrivate::TInt4<false>;
using TInt4 = NVectorDistance::NPrivate::TInt4<true>;

namespace NVectorDistance {
    template <typename T>
    inline constexpr bool IsInt4 = std::is_same_v<T, TInt4> || std::is_same_v<T, TUint4>;

    template <typename T>
    class TPackedInt4Coords {
    public:
        TPackedInt4Coords(const char* data, size_t size)
            : Data(reinterpret_cast<const ui8*>(data)), Size(size)
        {}

        struct TIterator {
            const ui8* Data;
            size_t Index;
            T operator*() const { return T::FromBits(Data[Index / 2] >> (4 * (Index % 2))); }
            TIterator& operator++() { ++Index; return *this; }
            bool operator!=(const TIterator& other) const { return Index != other.Index; }
        };

        TIterator begin() const { return {Data, 0}; }
        TIterator end() const { return {Data, Size}; }

    private:
        const ui8* Data;
        size_t Size;
    };
}

namespace std {
    template <bool Signed>
    struct numeric_limits<NVectorDistance::NPrivate::TInt4<Signed>> : numeric_limits<int> {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        static constexpr bool is_signed = Signed;
        static constexpr int digits = Signed ? 3 : 4;
        static constexpr int digits10 = Signed ? 0 : 1;
        static constexpr T epsilon() noexcept { return T(0); }
        static constexpr T round_error() noexcept { return T(0); }
        static constexpr T infinity() noexcept { return T(0); }
        static constexpr T quiet_NaN() noexcept { return T(0); }
        static constexpr T signaling_NaN() noexcept { return T(0); }
        static constexpr T denorm_min() noexcept { return T(0); }
        static constexpr T min() noexcept { return T(T::MinValue); }
        static constexpr T lowest() noexcept { return min(); }
        static constexpr T max() noexcept { return T(T::MaxValue); }
    };
}

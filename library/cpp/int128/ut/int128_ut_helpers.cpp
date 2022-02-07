#include "int128_ut_helpers.h"

namespace NInt128Private {
#if defined(_little_endian_)
    std::array<ui8, 16> GetAsArray(const ui128 value) {
        std::array<ui8, 16> result;
        const ui64 low = GetLow(value);
        const ui64 high = GetHigh(value);
        MemCopy(result.data(), reinterpret_cast<const ui8*>(&low), sizeof(low));
        MemCopy(result.data() + sizeof(low), reinterpret_cast<const ui8*>(&high), sizeof(high));
        return result;
    }

    std::array<ui8, 16> GetAsArray(const i128 value) {
        std::array<ui8, 16> result;
        const ui64 low = GetLow(value);
        const ui64 high = GetHigh(value);
        MemCopy(result.data(), reinterpret_cast<const ui8*>(&low), sizeof(low));
        MemCopy(result.data() + sizeof(low), reinterpret_cast<const ui8*>(&high), sizeof(high));
        return result;
    }
#elif defined(_big_endian_)
    std::array<ui8, 16> GetAsArray(const i128 value) {
        std::array<ui8, 16> result;
        const ui64 low = GetLow(value);
        const ui64 high = GetHigh(value);
        MemCopy(result.data(), reinterpret_cast<const ui8*>(&high), sizeof(high));
        MemCopy(result.data() + sizeof(high), reinterpret_cast<const ui8*>(&low), sizeof(low));
        return result;
    }

    std::array<ui8, 16> GetAsArray(const ui128 value) {
        std::array<ui8, 16> result;
        const ui64 low = GetLow(value);
        const ui64 high = GetHigh(value);
        MemCopy(result.data(), reinterpret_cast<const ui8*>(&high), sizeof(high));
        MemCopy(result.data() + sizeof(high), reinterpret_cast<const ui8*>(&low), sizeof(low));
        return result;
    }
#endif

#if defined(Y_HAVE_INT128)
    std::array<ui8, 16> GetAsArray(const unsigned __int128 value) {
        std::array<ui8, 16> result;
        MemCopy(result.data(), reinterpret_cast<const ui8*>(&value), sizeof(value));
        return result;
    }

    std::array<ui8, 16> GetAsArray(const signed __int128 value) {
        std::array<ui8, 16> result;
        MemCopy(result.data(), reinterpret_cast<const ui8*>(&value), sizeof(value));
        return result;
    }
#endif

}

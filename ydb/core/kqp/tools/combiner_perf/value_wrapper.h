#pragma once

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/string/printf.h>

#include <array>
#include <string>

namespace NKikimr {
namespace NMiniKQL {

// A simple wrapper for aggregate sums over both scalar values and tuples.
// Just a statically-sized array for now.

template<typename V, size_t Width>
struct TValueWrapper: public std::array<V, Width>
{
    using TElement = V;
    constexpr static const size_t ArrayWidth = Width;

    TValueWrapper()
        : std::array<V, Width>{}
    {
    };

    TValueWrapper& operator+= (const TValueWrapper& rhs)
    {
        for (size_t i = 0; i < Width; ++i) {
            (*this)[i] += rhs[i];
        }
        return *this;
    }

    static std::string Describe()
    {
        if constexpr (std::is_same_v<V, ui64>) {
            return Sprintf("array<ui64, %lu>", Width);
        } else if constexpr (std::is_same_v<V, std::string>) {
            return Sprintf("array<string, %lu>", Width);
        } else {
            return Sprintf("array<?, %lu>", Width);
        }
    }
};

}
}

template<typename V, size_t Width>
IOutputStream& operator << (IOutputStream& out Y_LIFETIME_BOUND, const NKikimr::NMiniKQL::TValueWrapper<V, Width>& wrapper) {
    for (size_t i = 0; i < Width; ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << wrapper[i];
    }
    return out;
}
#pragma once

#include <library/cpp/int128/int128.h>

#include <array>

namespace NInt128Private {
    std::array<ui8, 16> GetAsArray(const ui128 value);
    std::array<ui8, 16> GetAsArray(const i128 value);

#if defined(Y_HAVE_INT128)
    std::array<ui8, 16> GetAsArray(const unsigned __int128 value);
    std::array<ui8, 16> GetAsArray(const signed __int128 value);
#endif
}

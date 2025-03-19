#pragma once

#include <string_view>
#include "yql_wide_int.h"

#include <cstring>
#include <type_traits>
#include <limits>

namespace NYdb {
inline namespace Dev {
namespace NDecimal {

#ifdef _win_
#ifndef DONT_USE_NATIVE_INT128
#define DONT_USE_NATIVE_INT128
#endif
#endif

#ifdef DONT_USE_NATIVE_INT128
using TInt128 = TWide<int64_t>;
using TUint128 = TWide<uint64_t>;
#else
using TInt128 = signed __int128;
using TUint128 = unsigned __int128;
#endif

constexpr ui8 MaxPrecision = 35;

static_assert(sizeof(TInt128) == 16, "Wrong size of TInt128, expected 16");

inline constexpr TInt128 Inf() {
    return TInt128(100000000000000000ULL) * TInt128(1000000000000000000ULL);
}

inline constexpr TInt128 Nan() {
    return Inf() + TInt128(1);
}

inline constexpr TInt128 Err() {
    return Nan() + TInt128(1);
}

bool IsError(TInt128 v);
bool IsNan(TInt128 v);
bool IsInf(TInt128 v);

bool IsNormal(TInt128 v);

const char* ToString(TInt128 v, ui8 precision, ui8 scale = 0);
TInt128 FromString(const std::string_view& str, ui8 precision, ui8 scale = 0);
TInt128 FromHalfs(ui64 lo, i64 hi);

bool IsValid(const std::string_view& str);

}
}
}

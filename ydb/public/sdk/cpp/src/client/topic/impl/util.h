#pragma once

#include <concepts>
#include <string>
#include <yql/essentials/public/decimal/yql_decimal.h>

// Converts an integer to a string in a 256-character alphabets
template <typename Type>
    requires std::integral<Type>
void AsString(std::string& result, const Type value) {
#ifdef WORDS_BIGENDIAN
    result.append((const char*)&value, sizeof(Type));
#else
    auto b = (const char*)&value;
    for (auto* e = b + sizeof(Type); e != b;) {
        result.push_back(*(--e));
    }
#endif
}

template <typename Type>
    requires std::integral<Type>
std::string AsKeyBound(const Type value) {
    std::string key;
    key.reserve(sizeof(Type));
    AsString(key, value);
    return key;
}

// Specialization for TUint128
inline std::string AsKeyBound(const NYql::NDecimal::TUint128 value) {
    std::string key;
    key.reserve(16); // TUint128 is 16 bytes
    auto b = (const char*)&value;
    for (auto* e = b + 16; e != b;) {
        key.push_back(*(--e));
    }
    return key;
}

using THashKeyRange = std::pair<NYql::NDecimal::TUint128, NYql::NDecimal::TUint128>;

inline THashKeyRange RangeFromShardNumber(ui32 shardNumber, ui32 shardCount) {
    NYql::NDecimal::TUint128 max = -1;
    if (shardCount == 1) {
        return {0, max};
    }
    NYql::NDecimal::TUint128 slice = max / shardCount;
    NYql::NDecimal::TUint128 left = NYql::NDecimal::TUint128(shardNumber) * slice;
    NYql::NDecimal::TUint128 right =
            shardNumber + 1 == shardCount ? max : NYql::NDecimal::TUint128(shardNumber + 1) * slice -
                                                  NYql::NDecimal::TUint128(1);
    return {left, right};
}

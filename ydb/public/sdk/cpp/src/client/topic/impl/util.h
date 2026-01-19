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

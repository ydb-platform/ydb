#pragma once

#include <ydb/library/vector_distance/float16.h>

#include "util/system/types.h"

enum EFormat: ui8 {
    FloatVector = 1,    // 4-byte per element
    Uint8Vector = 2,    // 1-byte per element, better than Int8 for positive-only Float
    Int8Vector = 3,     // 1-byte per element
    Float16Vector = 4,   // 2-byte per element, IEEE-754 binary16
    BFloat16Vector = 5,  // 2-byte per element, bfloat16
    BitVector = 10,      // 1-bit  per element
};

template <typename T>
struct TTypeToFormat;

template <>
struct TTypeToFormat<float> {
    static constexpr auto Format = EFormat::FloatVector;
};

template <>
struct TTypeToFormat<TFloat16> {
    static constexpr auto Format = EFormat::Float16Vector;
};

template <>
struct TTypeToFormat<TBFloat16> {
    static constexpr auto Format = EFormat::BFloat16Vector;
};

template <>
struct TTypeToFormat<i8> {
    static constexpr auto Format = EFormat::Int8Vector;
};

template <>
struct TTypeToFormat<ui8> {
    static constexpr auto Format = EFormat::Uint8Vector;
};

template <>
struct TTypeToFormat<bool> {
    static constexpr auto Format = EFormat::BitVector;
};

template <typename T>
inline constexpr auto Format = TTypeToFormat<T>::Format;
inline constexpr auto HeaderLen = sizeof(ui8);

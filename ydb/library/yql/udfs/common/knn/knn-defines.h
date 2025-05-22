#pragma once

#include "util/system/types.h"

enum EFormat: ui8 {
    FloatVector = 1, // 4-byte per element
    Uint8Vector = 2, // 1-byte per element, better than Int8 for positive-only Float
    Int8Vector = 3,  // 1-byte per element
    BitVector = 10,  // 1-bit  per element
};

template <typename T>
struct TTypeToFormat;

template <>
struct TTypeToFormat<float> {
    static constexpr auto Format = EFormat::FloatVector;
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

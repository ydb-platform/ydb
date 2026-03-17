/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#pragma once

#include <array>
#include <climits>  // CHAR_BIT

// NumericLimits is a class template that provides the minimum and maximum values for a given number of bits.
// The minimum and maximum values are calculated for both signed and unsigned integral types.

template <typename ValueType, bool = std::is_signed<ValueType>::value, bool = std::is_integral<ValueType>::value>
struct NumericLimits
{
    static_assert(std::is_integral<ValueType>::value, "ValueType must be an integral type");
};


// Example:
// For a 16-bit signed integer, the minimum and maximum values are:
// nBits | min | max
// ----- | --- | ---
// 1     | -1  | 0
// 2     | -2  | 1
// 3     | -4  | 3
// 4     | -8  | 7
// ...
// 15    | -16384 | 16383
// 16    | -32768 | 32767

template <typename ValueType>
struct NumericLimits<ValueType, true, true>
{
private:
    static constexpr std::size_t maxNBits = CHAR_BIT * sizeof(ValueType);

    static constexpr std::array<ValueType, maxNBits> max_ = []() {
        std::array<ValueType, maxNBits> max{};
        using UnsignedValueType          = std::make_unsigned_t<ValueType>;
        constexpr UnsignedValueType ones = ~UnsignedValueType{ 0 };
        max[0]                           = 0;
        for (std::size_t i = 1; i < maxNBits; ++i) {
            max[i] = static_cast<ValueType>(ones >> (maxNBits - i));
        }
        return max;
    }();

    static constexpr std::array<ValueType, maxNBits> min_ = []() {
        std::array<ValueType, maxNBits> min{};
        for (std::size_t i = 0; i < maxNBits; ++i) {
            min[i] = ~max_[i];
        }
        return min;
    }();

public:
    static constexpr ValueType min(std::size_t nBits) { return min_[nBits - 1]; }
    static constexpr ValueType max(std::size_t nBits) { return max_[nBits - 1]; }
};


// Example:
// For a 16-bit unsigned integer, the minimum and maximum values are:
// nBits | min | max
// ----- | --- | ---
// 1     | 0   | 1
// 2     | 0   | 3
// 3     | 0   | 7
// 4     | 0   | 15
// ...
// 15    | 0   | 32767
// 16    | 0   | 65535

template <typename ValueType>
struct NumericLimits<ValueType, false, true>
{
private:
    static constexpr std::size_t maxNBits = CHAR_BIT * sizeof(ValueType);

    static constexpr std::array<ValueType, maxNBits> max_ = []() {
        std::array<ValueType, maxNBits> max{};
        constexpr ValueType ones = ~(static_cast<ValueType>(0));
        max[0]                   = 1;
        for (std::size_t i = 1; i < maxNBits; ++i) {
            max[i] = ones >> (maxNBits - i - 1);
        }
        return max;
    }();

public:
    static constexpr ValueType min(std::size_t nBits) { return 0; }
    static constexpr ValueType max(std::size_t nBits) { return max_[nBits - 1]; }
};

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

#include "grib_api_internal.h"
#include "grib_scaling.h"

#include <array>
#include <type_traits>
#include <cstdint>

template <typename T> int grib_ieee_decode_array(grib_context* c, unsigned char* buf, size_t nvals, int bytes, T* val);

/**
.. _init_ieee_table:

Init IEEE Table
===============

Initializes the ieee_table with IEEE754 single precision (32-bit) values. Nearest smaller values (e.g., reference values for grid_simple and grid_ccsds) are taken from this table.

Details
-------

The table layout is as follows:

+-------+----------------+------------------------+
| idx (i) | multiplier (e) | value (v = mmin * e) |
+-------+----------------+------------------------+
| 0     | 0              | 0                      |
| 1     | 2^(-149)       | 0x800000 * 2^(-149)    |
| 2     | 2^(-148)       | 0x800000 * 2^(-148)    |
| ...   | ...            | ...                    |
| 253   | 2^103          | 0x800000 * 2^103       |
| 254   | 2^104          | 0x800000 * 2^104       |
+-------+----------------+------------------------+

The vmin and vmax boundaries are defined as:

- vmin =  0x800000 * 2^(-149)
- vmax =  0xffffff * 2^104
*/

template <typename ValueType>
struct IeeeTable {
private:
    static_assert(std::is_floating_point<ValueType>::value, "ValueType must be a floating point type");
    static constexpr int TABLESIZE = 255;
    static constexpr uint32_t mantissa_min = 0x800000;
    static constexpr uint32_t mantissa_max = 0xffffff;

public:
    static constexpr std::array<ValueType, TABLESIZE> e = []() {
        std::array<ValueType, TABLESIZE> multiplier{};
        multiplier[0] = 0;
        for (int i = 1; i < TABLESIZE; ++i) {
            multiplier[i] = codes_power<ValueType>(i - 150, 2);
        }
        return multiplier;
    }();
    static constexpr std::array<ValueType, TABLESIZE> v = []() {
        std::array<ValueType, TABLESIZE> values{};
        values[0] = 0;
        for (int i = 1; i < TABLESIZE; ++i) {
            values[i] = e[i] * mantissa_min;
        }
        return values;
    }();
    static constexpr ValueType vmin = e[1] * mantissa_min;
    static constexpr ValueType vmax = e[254] * mantissa_max;
};

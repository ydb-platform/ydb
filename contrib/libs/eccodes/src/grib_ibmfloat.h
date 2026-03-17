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

#include "grib_scaling.h"

#include <array>
#include <type_traits>
#include <cstdint>

/**
.. _init_ieee_table:

Init IBM Floats Table
=====================

Initializes the ibm_table with IBM Float values. Nearest smaller values (e.g., reference values for grid_simple) are taken from this table.

Details
-------

The table layout is as follows:

+-------+----------------+------------------------+
| idx (i) | multiplier (e) | value (v = mmin * e) |
+-------+----------------+------------------------+
| 0     | 16^(-70)       | 0x100000 * 16^(-70)     |
| 1     | 16^(-69)       | 0x100000 * 16^(-69)     |
| ...   | ...            | ...                    |
| 126   | 16^56          | 0x100000 * 16^56        |
| 127   | 16^57          | 0x100000 * 16^57        |
+-------+----------------+------------------------+

The vmin and vmax boundaries are defined as:

- vmin =  0x100000 * 16^(-70)
- vmax =  0xffffff * 16^57
*/

struct IbmTable
{
private:
    using ValueType                        = double;
    static constexpr int TABLESIZE         = 128;
    static constexpr uint32_t mantissa_min = 0x100000;
    static constexpr uint32_t mantissa_max = 0xffffff;

public:
    static constexpr std::array<ValueType, TABLESIZE> e = []() {
        std::array<ValueType, TABLESIZE> multiplier{};
        multiplier[0] = 0;
        for (int i = 0; i < TABLESIZE; ++i) {
            multiplier[i] = codes_power<ValueType>(i - 70, 16);
        }
        return multiplier;
    }();
    static constexpr std::array<ValueType, TABLESIZE> v = []() {
        std::array<ValueType, TABLESIZE> values{};
        values[0] = 0;
        for (int i = 0; i < TABLESIZE; ++i) {
            values[i] = e[i] * mantissa_min;
        }
        return values;
    }();
    static constexpr ValueType vmin = e[0] * mantissa_min;
    static constexpr ValueType vmax = e[127] * mantissa_max;
};

/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_scaling.h"
#include "grib_api_internal.h"

long grib_op_eq(long a, long b)
{
    return a == b;
}
long grib_op_ne(long a, long b)
{
    return a != b;
}
long grib_op_lt(long a, long b)
{
    return a < b;
}
long grib_op_gt(long a, long b)
{
    return a > b;
}
// long grib_op_and(long a, long b)
// {
//     return a && b;
// }
// long grib_op_or(long a, long b)
// {
//     return a || b;
// }
long grib_op_ge(long a, long b)
{
    return a >= b;
}
long grib_op_le(long a, long b)
{
    return a <= b;
}
long grib_op_bit(long a, long b)
{
    return a & (1 << b);
}
long grib_op_bitoff(long a, long b)
{
    return !grib_op_bit(a, b);
}
long grib_op_not(long a)
{
    return !a;
}
long grib_op_neg(long a)
{
    return -a;
}
double grib_op_neg_d(double a)
{
    return -a;
}

// Note: This is actually 'a' to the power 'b'
long grib_op_pow(long a, long b)
{
    return codes_power<double>(b, a);
}

long grib_op_add(long a, long b)
{
    return a + b;
}
long grib_op_sub(long a, long b)
{
    return a - b;
}
long grib_op_div(long a, long b)
{
    return a / b;
}
long grib_op_mul(long a, long b)
{
    return a * b;
}
long grib_op_modulo(long a, long b)
{
    return a % b;
}
double grib_op_mul_d(double a, double b)
{
    return a * b;
}
double grib_op_div_d(double a, double b)
{
    return a / b;
}
double grib_op_add_d(double a, double b)
{
    return a + b;
}
double grib_op_sub_d(double a, double b)
{
    return a - b;
}
double grib_op_eq_d(double a, double b)
{
    return a == b;
}
double grib_op_ne_d(double a, double b)
{
    return a != b;
}
double grib_op_lt_d(double a, double b)
{
    return a < b;
}
double grib_op_gt_d(double a, double b)
{
    return a > b;
}
double grib_op_ge_d(double a, double b)
{
    return a >= b;
}
double grib_op_le_d(double a, double b)
{
    return a <= b;
}

// #define LOOKUP(a) if (proc == a) { return "&" #a; }
// const char* grib_binop_long_proc_name(const grib_binop_long_proc proc)
// {
//     if (!proc)
//         return "NULL";
//     LOOKUP(grib_op_eq);
//     LOOKUP(grib_op_ne);
//     LOOKUP(grib_op_lt);
//     LOOKUP(grib_op_gt);
//     LOOKUP(grib_op_and);
//     LOOKUP(grib_op_or);
//     LOOKUP(grib_op_ge);
//     LOOKUP(grib_op_le);
//     LOOKUP(grib_op_bit);
//     LOOKUP(grib_op_bitoff);
//     LOOKUP(grib_op_pow);
//     LOOKUP(grib_op_add);
//     LOOKUP(grib_op_sub);
//     LOOKUP(grib_op_div);
//     LOOKUP(grib_op_mul);
//     LOOKUP(grib_op_modulo);
//     fprintf(stderr, "Cannot find grib_binop_long_proc\n");
//     ECCODES_ASSERT(0);
//     return NULL;
// }

// const char* grib_binop_double_proc_name(const grib_binop_double_proc proc)
// {
//     if (!proc)
//         return "NULL";
//     LOOKUP(grib_op_mul_d);
//     LOOKUP(grib_op_div_d);
//     LOOKUP(grib_op_add_d);
//     LOOKUP(grib_op_sub_d);
//     LOOKUP(grib_op_eq_d);
//     LOOKUP(grib_op_ne_d);
//     LOOKUP(grib_op_lt_d);
//     LOOKUP(grib_op_gt_d);
//     LOOKUP(grib_op_ge_d);
//     LOOKUP(grib_op_le_d);
//     fprintf(stderr, "Cannot find grib_binop_double_proc_name\n");
//     ECCODES_ASSERT(0);
//     return NULL;
// }

// const char* grib_unop_long_proc_name(const grib_unop_long_proc proc)
// {
//     if (!proc)
//         return "NULL";
//     LOOKUP(grib_op_not);
//     LOOKUP(grib_op_neg);
//     fprintf(stderr, "Cannot find grib_unop_long_proc_name\n");
//     ECCODES_ASSERT(0);
//     return NULL;
// }

// const char* grib_unop_double_proc_name(const grib_unop_double_proc proc)
// {
//     if (!proc)
//         return "NULL";
//     LOOKUP(grib_op_neg_d);
//     fprintf(stderr, "Cannot find grib_unop_double_proc_name\n");
//     ECCODES_ASSERT(0);
//     return NULL;
// }

/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#ifndef MY_ISNAN_ISINF_H
#define MY_ISNAN_ISINF_H
#include <cstdint>
#include <stdexcept>

/*
 * IEEE 754 compliant simple functions to test for NANs and INFs.
 * This functions are necessary, b/c isnan doesn't work with -Ofast -ffast-math flags
 * see http://searchivarius.org/blog/gcc-disables-isnan-and-isinf-when-compiling-ffast-math-flag  
 *
 * See also test files. The file ./regular is compiled without -ffast-math flag and it
 * checks for a large number of values that the output of my functions is the same
 * as the output of standard functions. For single precision numbers, i.e., for floats
 * these checks are exhaustive. That is, I go over the set of all 4B+ possible float values.
 * For doubles, this is not possible, so I use tests where the lower 32 bits of mantissa
 * are set to zero.
 * 
 * Copyright (c) 2017   Leonid Boytsov
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */

// A mask to extract an exponent from the single-precision floating point number
// 01111111100000000000000000000000 
const unsigned FLOAT_EXP_MASK = 0x7F800000; 
// A mask to extract a mantissa/fractional part from the single-precision floating point number
// 00000000011111111111111111111111 
const unsigned FLOAT_FRAC_PART_MASK = 0x7FFFFF;

inline bool my_isnan(float x) {
  union {
    uint32_t u;
    float    f;
  } conv;
  conv.f = x;
  return ((conv.u & FLOAT_EXP_MASK) == FLOAT_EXP_MASK) && ((conv.u & FLOAT_FRAC_PART_MASK) != 0);
};

inline bool my_isinf(float x) {
  union {
    uint32_t u;
    float    f;
  } conv;
  conv.f = x;
  return ((conv.u & FLOAT_EXP_MASK) == FLOAT_EXP_MASK) && ((conv.u & FLOAT_FRAC_PART_MASK) == 0);
};

// 0111 1111 1111 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000
const uint64_t DOUBLE_EXP_MASK = 0x7FF0000000000000ul;
// 0000 0000 0000 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111
const uint64_t DOUBLE_FRAC_PART_MASK = 0x000FFFFFFFFFFFFFul;

inline bool my_isnan(double x) {
  union {
    uint64_t  u;
    double    f;
  } conv;
  conv.f = x;
  return ((conv.u & DOUBLE_EXP_MASK) == DOUBLE_EXP_MASK) && ((conv.u & DOUBLE_FRAC_PART_MASK) != 0);
};


inline bool my_isinf(double x) {
  union {
    uint64_t  u;
    double    f;
  } conv;
  conv.f = x;
  return ((conv.u & DOUBLE_EXP_MASK) == DOUBLE_EXP_MASK) && ((conv.u & DOUBLE_FRAC_PART_MASK) == 0);
};

inline bool my_isnan(int x) {
  return false;
}

inline bool my_isinf(int x) {
  return false;
}

inline bool my_isnan(long double x) {
  throw runtime_error("my is_nan is not supported for long double!");
}
inline bool my_isinf(long double x) {
  throw runtime_error("my_isinf is not supported for long double!");
}

#endif

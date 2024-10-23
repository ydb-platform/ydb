/*******************************************************************************
 * tlx/math/ffs.hpp
 *
 * ffs() find first set bit in integer - mainly for portability as ffs() is a
 * glibc extension and not available on Visual Studio.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_FFS_HEADER
#define TLX_MATH_FFS_HEADER

namespace tlx {

//! \addtogroup tlx_math
//! \{

/******************************************************************************/
// ffs() - find first set bit in integer

//! ffs (find first set bit) - generic implementation
template <typename Integral>
static inline unsigned ffs_template(Integral x) {
    if (x == 0) return 0u;
    unsigned r = 1;
    while ((x & 1) == 0)
        x >>= 1, ++r;
    return r;
}

/******************************************************************************/

#if defined(__GNUC__) || defined(__clang__)

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(int i) {
    return static_cast<unsigned>(__builtin_ffs(i));
}

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(unsigned i) {
    return ffs(static_cast<int>(i));
}

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(long i) {
    return static_cast<unsigned>(__builtin_ffsl(i));
}

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(unsigned long i) {
    return ffs(static_cast<long>(i));
}

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(long long i) {
    return static_cast<unsigned>(__builtin_ffsll(i));
}

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(unsigned long long i) {
    return ffs(static_cast<long long>(i));
}

#else

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(int i) {
    return ffs_template(i);
}

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(unsigned int i) {
    return ffs_template(i);
}

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(long i) {
    return ffs_template(i);
}

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(unsigned long i) {
    return ffs_template(i);
}

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(long long i) {
    return ffs_template(i);
}

//! find first set bit in integer, or zero if none are set.
static inline
unsigned ffs(unsigned long long i) {
    return ffs_template(i);
}

#endif

//! \}

} // namespace tlx

#endif // !TLX_MATH_FFS_HEADER

/******************************************************************************/

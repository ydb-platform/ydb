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
#pragma once

/* 
 * Note that __GNUC__ is also defined for Intel and Clang,
 * which do understand __attribute__ ((aligned(16)))
 */

#if defined(__GNUC__)
#define PORTABLE_ALIGN16 __attribute__((aligned(16)))
#else
#define PORTABLE_ALIGN16 __declspec(align(16))
#endif
#if defined(__GNUC__)
#define PORTABLE_ALIGN32 __attribute__((aligned(32)))
#else
#define PORTABLE_ALIGN32 __declspec(align(32))
#endif

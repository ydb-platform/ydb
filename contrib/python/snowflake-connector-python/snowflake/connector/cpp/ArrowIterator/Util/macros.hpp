//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_UTIL_MACROS_HPP
#define PC_UTIL_MACROS_HPP

/** the same macros as linux kernel's likely and unlikely. can help specific
 * compiler to make branch prediction */
#if defined(__GNUC__)
#define LIKELY(x) (__builtin_expect(!!(x), 1))
#define UNLIKELY(x) (__builtin_expect(!!(x), 0))
#else
#define LIKELY(x) x
#define UNLIKELY(x) x
#endif  // __GNUC__

#endif  // PC_UTIL_MACROS_HPP

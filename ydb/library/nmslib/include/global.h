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
#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

static const int kMaxFilenameLength = 255;

#define ARRAY_SIZE(x) (static_cast<int>(sizeof(x)/sizeof(*x)))

#ifdef __GNUC__
#define DEPRECATED(func)  __attribute__ ((deprecated)) func
#elif defined(_MSC_VER)
#define DEPRECATED(func)  __declspec(deprecated) func
#else
#pragma message("WARNING: You need to implement DEPRECATED for this compiler")
#define DEPRECATED(func)  func
#endif

namespace similarity {

#undef DISABLE_COPY_AND_ASSIGN
#define DISABLE_COPY_AND_ASSIGN(Type) \
  explicit Type(const Type&); \
  Type& operator=(const Type&)

/*
 * On windows, disabling copy&assign generates way to many warnings C4661
 * Let's disable this warning.
 */
#if defined(_MSC_VER)
  #pragma warning(disable: 4661)
#endif

}   // namespace similarity

#endif    // _GLOBAL_H_


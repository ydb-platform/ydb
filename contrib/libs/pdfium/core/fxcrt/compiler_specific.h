// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_COMPILER_SPECIFIC_H_
#define CORE_FXCRT_COMPILER_SPECIFIC_H_

#include "build/build_config.h"

// A wrapper around `__has_attribute`, similar to HAS_CPP_ATTRIBUTE.
#if defined(__has_attribute)
#define HAS_ATTRIBUTE(x) __has_attribute(x)
#else
#define HAS_ATTRIBUTE(x) 0
#endif

// Annotate a function indicating it should not be inlined.
// Use like:
//   NOINLINE void DoStuff() { ... }
#if defined(__clang__) && HAS_ATTRIBUTE(noinline)
#define NOINLINE [[clang::noinline]]
#elif defined(COMPILER_GCC) && HAS_ATTRIBUTE(noinline)
#define NOINLINE __attribute__((noinline))
#elif defined(COMPILER_MSVC)
#define NOINLINE __declspec(noinline)
#else
#define NOINLINE
#endif

// Macro for hinting that an expression is likely to be false.
#if !defined(UNLIKELY)
#if defined(COMPILER_GCC) || defined(__clang__)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define UNLIKELY(x) (x)
#endif  // defined(COMPILER_GCC)
#endif  // !defined(UNLIKELY)

#if !defined(LIKELY)
#if defined(COMPILER_GCC) || defined(__clang__)
#define LIKELY(x) __builtin_expect(!!(x), 1)
#else
#define LIKELY(x) (x)
#endif  // defined(COMPILER_GCC)
#endif  // !defined(LIKELY)

// Marks a type as being eligible for the "trivial" ABI despite having a
// non-trivial destructor or copy/move constructor. Such types can be relocated
// after construction by simply copying their memory, which makes them eligible
// to be passed in registers. The canonical example is std::unique_ptr.
//
// Use with caution; this has some subtle effects on constructor/destructor
// ordering and will be very incorrect if the type relies on its address
// remaining constant. When used as a function argument (by value), the value
// may be constructed in the caller's stack frame, passed in a register, and
// then used and destructed in the callee's stack frame. A similar thing can
// occur when values are returned.
//
// TRIVIAL_ABI is not needed for types which have a trivial destructor and
// copy/move constructors, such as base::TimeTicks and other POD.
//
// It is also not likely to be effective on types too large to be passed in one
// or two registers on typical target ABIs.
//
// See also:
//   https://clang.llvm.org/docs/AttributeReference.html#trivial-abi
//   https://libcxx.llvm.org/docs/DesignDocs/UniquePtrTrivialAbi.html
#if defined(__clang__) && HAS_ATTRIBUTE(trivial_abi)
#define TRIVIAL_ABI [[clang::trivial_abi]]
#else
#define TRIVIAL_ABI
#endif

#if defined(__clang__)
#define GSL_POINTER [[gsl::Pointer]]
#else
#define GSL_POINTER
#endif

#if defined(__clang__) && HAS_ATTRIBUTE(unsafe_buffer_usage)
#define UNSAFE_BUFFER_USAGE [[clang::unsafe_buffer_usage]]
#else
#define UNSAFE_BUFFER_USAGE
#endif

// clang-format off
// Formatting is off so that we can put each _Pragma on its own line, as
// recommended by the gcc docs.
#if defined(PDF_USE_CHROME_PLUGINS)
#define UNSAFE_BUFFERS(...)                  \
  _Pragma("clang unsafe_buffer_usage begin") \
  __VA_ARGS__                                \
  _Pragma("clang unsafe_buffer_usage end")
#else
#define UNSAFE_BUFFERS(...) __VA_ARGS__
#endif
// clang-format on

// Like UNSAFE_BUFFERS(), but indicates there is a TODO() task to
// investigate safety,
// TODO(crbug.com/pdfium/2155): remove all usage.
#define UNSAFE_TODO(...) UNSAFE_BUFFERS(__VA_ARGS__)

#endif  // CORE_FXCRT_COMPILER_SPECIFIC_H_

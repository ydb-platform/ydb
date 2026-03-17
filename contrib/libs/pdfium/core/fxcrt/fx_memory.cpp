// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/fx_memory.h"

#include <stdint.h>  // For uintptr_t.
#include <stdlib.h>  // For abort().

#include <iterator>
#include <limits>
#include <type_traits>

#include "build/build_config.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/debug/alias.h"

#if BUILDFLAG(IS_WIN)
#include <windows.h>
#endif

#if BUILDFLAG(IS_ANDROID)
#include <malloc.h>
#endif

namespace {

#if DCHECK_IS_ON()
// TODO(thestig): When C++20 is required, replace with std::has_single_bit().
// Returns true iff |value| is a power of 2.
template <typename T, typename = std::enable_if<std::is_integral<T>::value>>
constexpr inline bool IsPowerOfTwo(T value) {
  // From "Hacker's Delight": Section 2.1 Manipulating Rightmost Bits.
  //
  // Only positive integers with a single bit set are powers of two. If only one
  // bit is set in x (e.g. 0b00000100000000) then |x-1| will have that bit set
  // to zero and all bits to its right set to 1 (e.g. 0b00000011111111). Hence
  // |x & (x-1)| is 0 iff x is a power of two.
  return value > 0 && (value & (value - 1)) == 0;
}

#ifdef __has_builtin
#define SUPPORTS_BUILTIN_IS_ALIGNED (__has_builtin(__builtin_is_aligned))
#else
#define SUPPORTS_BUILTIN_IS_ALIGNED 0
#endif

inline bool IsAligned(void* val, size_t alignment) {
  // If the compiler supports builtin alignment checks prefer them.
#if SUPPORTS_BUILTIN_IS_ALIGNED
  return __builtin_is_aligned(reinterpret_cast<uintptr_t>(val), alignment);
#else
  DCHECK(IsPowerOfTwo(alignment));
  return (reinterpret_cast<uintptr_t>(val) & (alignment - 1)) == 0;
#endif
}

#undef SUPPORTS_BUILTIN_IS_ALIGNED

#endif  // DCHECK_IS_ON()

}  // namespace

void* FXMEM_DefaultAlloc(size_t byte_size) {
  return pdfium::internal::Alloc(byte_size, 1);
}

void* FXMEM_DefaultCalloc(size_t num_elems, size_t byte_size) {
  return pdfium::internal::Calloc(num_elems, byte_size);
}

void* FXMEM_DefaultRealloc(void* pointer, size_t new_size) {
  return pdfium::internal::Realloc(pointer, new_size, 1);
}

void FXMEM_DefaultFree(void* pointer) {
  FX_Free(pointer);
}

NOINLINE void FX_OutOfMemoryTerminate(size_t size) {
  // Convince the linker this should not be folded with similar functions using
  // Identical Code Folding.
  static int make_this_function_aliased = 0xbd;
  pdfium::Alias(&make_this_function_aliased);

#if BUILDFLAG(IS_WIN)
  // The same custom Windows exception code used in Chromium and Breakpad.
  constexpr DWORD kOomExceptionCode = 0xe0000008;
  ULONG_PTR exception_args[] = {size};
  ::RaiseException(kOomExceptionCode, EXCEPTION_NONCONTINUABLE,
                   std::size(exception_args), exception_args);
#endif

  // Terminate cleanly.
  abort();
}

void* FX_AlignedAlloc(size_t size, size_t alignment) {
  DCHECK_GT(size, 0u);
  DCHECK(IsPowerOfTwo(alignment));
  DCHECK_EQ(alignment % sizeof(void*), 0u);
  void* ptr = nullptr;
#if defined(COMPILER_MSVC)
  ptr = _aligned_malloc(size, alignment);
#elif BUILDFLAG(IS_ANDROID)
  // Android technically supports posix_memalign(), but does not expose it in
  // the current version of the library headers used by Chrome.  Luckily,
  // memalign() on Android returns pointers which can safely be used with
  // free(), so we can use it instead.  Issue filed to document this:
  // http://code.google.com/p/android/issues/detail?id=35391
  ptr = memalign(alignment, size);
#else
  int ret = posix_memalign(&ptr, alignment, size);
  if (ret != 0) {
    ptr = nullptr;
  }
#endif

  // Since aligned allocations may fail for non-memory related reasons, force a
  // crash if we encounter a failed allocation; maintaining consistent behavior
  // with a normal allocation failure in Chrome.
  if (!ptr) {
    CHECK(false);
  }
  // Sanity check alignment just to be safe.
  DCHECK(IsAligned(ptr, alignment));
  return ptr;
}

namespace pdfium::internal {

void* Alloc2D(size_t w, size_t h, size_t member_size) {
  if (w >= std::numeric_limits<size_t>::max() / h) {
    return nullptr;
  }

  return Alloc(w * h, member_size);
}

void* AllocOrDie(size_t num_members, size_t member_size) {
  void* result = Alloc(num_members, member_size);
  if (!result)
    FX_OutOfMemoryTerminate(0);  // Never returns.

  return result;
}

void* AllocOrDie2D(size_t w, size_t h, size_t member_size) {
  if (w >= std::numeric_limits<size_t>::max() / h)
    FX_OutOfMemoryTerminate(0);  // Never returns.

  return AllocOrDie(w * h, member_size);
}
void* CallocOrDie(size_t num_members, size_t member_size) {
  void* result = Calloc(num_members, member_size);
  if (!result)
    FX_OutOfMemoryTerminate(0);  // Never returns.

  return result;
}

void* CallocOrDie2D(size_t w, size_t h, size_t member_size) {
  if (w >= std::numeric_limits<size_t>::max() / h)
    FX_OutOfMemoryTerminate(0);  // Never returns.

  return CallocOrDie(w * h, member_size);
}

void* ReallocOrDie(void* ptr, size_t num_members, size_t member_size) {
  void* result = Realloc(ptr, num_members, member_size);
  if (!result)
    FX_OutOfMemoryTerminate(0);  // Never returns.

  return result;
}

void* StringAllocOrDie(size_t num_members, size_t member_size) {
  void* result = StringAlloc(num_members, member_size);
  if (!result)
    FX_OutOfMemoryTerminate(0);  // Never returns.

  return result;
}

}  // namespace pdfium::internal

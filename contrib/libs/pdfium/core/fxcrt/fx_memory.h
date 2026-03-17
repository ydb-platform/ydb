// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FX_MEMORY_H_
#define CORE_FXCRT_FX_MEMORY_H_

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// For external C libraries to malloc through PDFium. These may return nullptr.
void* FXMEM_DefaultAlloc(size_t byte_size);
void* FXMEM_DefaultCalloc(size_t num_elems, size_t byte_size);
void* FXMEM_DefaultRealloc(void* pointer, size_t new_size);
void FXMEM_DefaultFree(void* pointer);

#ifdef __cplusplus
}  // extern "C"

#include "core/fxcrt/compiler_specific.h"

#if defined(COMPILER_MSVC)
#include <malloc.h>
#else
#include <stdlib.h>
#endif

void FX_InitializeMemoryAllocators();
void FX_DestroyMemoryAllocators();
NOINLINE void FX_OutOfMemoryTerminate(size_t size);

// General Partition Allocators.

// These never return nullptr, and must return cleared memory.
#define FX_Alloc(type, size) \
  static_cast<type*>(pdfium::internal::CallocOrDie(size, sizeof(type)))
#define FX_Alloc2D(type, w, h) \
  static_cast<type*>(pdfium::internal::CallocOrDie2D(w, h, sizeof(type)))
#define FX_Realloc(type, ptr, size) \
  static_cast<type*>(pdfium::internal::ReallocOrDie(ptr, size, sizeof(type)))

// May return nullptr, but returns cleared memory otherwise.
#define FX_TryAlloc(type, size) \
  static_cast<type*>(pdfium::internal::Calloc(size, sizeof(type)))
#define FX_TryRealloc(type, ptr, size) \
  static_cast<type*>(pdfium::internal::Realloc(ptr, size, sizeof(type)))

// These never return nullptr, but return uninitialized memory.
#define FX_AllocUninit(type, size) \
  static_cast<type*>(pdfium::internal::AllocOrDie(size, sizeof(type)))
#define FX_AllocUninit2D(type, w, h) \
  static_cast<type*>(pdfium::internal::AllocOrDie2D(w, h, sizeof(type)))

// May return nullptr, but returns uninitialized memory otherwise.
#define FX_TryAllocUninit(type, size) \
  static_cast<type*>(pdfium::internal::Alloc(size, sizeof(type)))
#define FX_TryAllocUninit2D(type, w, h) \
  static_cast<type*>(pdfium::internal::Alloc2D(w, h, sizeof(type)))

// FX_Free frees memory from the above.
#define FX_Free(ptr) pdfium::internal::Dealloc(ptr)

// String Partition Allocators.

// This never returns nullptr, but returns uninitialized memory.
#define FX_StringAlloc(type, size) \
  static_cast<type*>(pdfium::internal::StringAllocOrDie(size, sizeof(type)))

// FX_StringFree frees memory from FX_StringAlloc.
#define FX_StringFree(ptr) pdfium::internal::StringDealloc(ptr)

#ifndef V8_ENABLE_SANDBOX
// V8 Array Buffer Partition Allocators.

// This never returns nullptr, and returns zeroed memory.
void* FX_ArrayBufferAllocate(size_t length);

// This never returns nullptr, but returns uninitialized memory.
void* FX_ArrayBufferAllocateUninitialized(size_t length);

// FX_ArrayBufferFree accepts memory from both of the above.
void FX_ArrayBufferFree(void* data);
#endif  // V8_ENABLE_SANDBOX

// Aligned allocators.

// This can be replaced with std::aligned_alloc when we have C++17.
// Caveat: std::aligned_alloc requires the size parameter be an integral
// multiple of alignment.
void* FX_AlignedAlloc(size_t size, size_t alignment);

inline void FX_AlignedFree(void* ptr) {
#if defined(COMPILER_MSVC)
  _aligned_free(ptr);
#else
  free(ptr);
#endif
}

namespace pdfium {
namespace internal {

// General partition.
void* Alloc(size_t num_members, size_t member_size);
void* Alloc2D(size_t w, size_t h, size_t member_size);
void* AllocOrDie(size_t num_members, size_t member_size);
void* AllocOrDie2D(size_t w, size_t h, size_t member_size);
void* Calloc(size_t num_members, size_t member_size);
void* Realloc(void* ptr, size_t num_members, size_t member_size);
void* CallocOrDie(size_t num_members, size_t member_size);
void* CallocOrDie2D(size_t w, size_t h, size_t member_size);
void* ReallocOrDie(void* ptr, size_t num_members, size_t member_size);
void Dealloc(void* ptr);

// String partition.
void* StringAlloc(size_t num_members, size_t member_size);
void* StringAllocOrDie(size_t num_members, size_t member_size);
void StringDealloc(void* ptr);

}  // namespace internal
}  // namespace pdfium

// Force stack allocation of a class. Classes that do complex work in a
// destructor, such as the flushing of buffers, should be declared as
// stack-allocated as possible, since future memory allocation schemes
// may not run destructors in a predictable manner if an instance is
// heap-allocated.
#define FX_STACK_ALLOCATED()           \
  void* operator new(size_t) = delete; \
  void* operator new(size_t, void*) = delete

// Round up to the power-of-two boundary N.
template <int N, typename T>
inline T FxAlignToBoundary(T size) {
  static_assert(N > 0 && (N & (N - 1)) == 0, "Not non-zero power of two");
  return (size + (N - 1)) & ~(N - 1);
}

#endif  // __cplusplus

#endif  // CORE_FXCRT_FX_MEMORY_H_

// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/fx_memory.h"

#include <stdlib.h>

#include <limits>

#include "build/build_config.h"
#include "core/fxcrt/fx_safe_types.h"

#if defined(PDF_USE_PARTITION_ALLOC)
#error "File compiled under wrong build option."
#endif

namespace pdfium {
namespace internal {

// Slightly less than 2GB, typically.
constexpr size_t kMallocSizeLimit = std::numeric_limits<int>::max() - (1 << 12);

void* Alloc(size_t num_members, size_t member_size) {
  FX_SAFE_SIZE_T total = member_size;
  total *= num_members;
  if (!total.IsValid() || total.ValueOrDie() >= kMallocSizeLimit)
    return nullptr;
  return malloc(total.ValueOrDie());
}

void* Calloc(size_t num_members, size_t member_size) {
  FX_SAFE_SIZE_T total = member_size;
  total *= num_members;
  if (!total.IsValid() || total.ValueOrDie() >= kMallocSizeLimit)
    return nullptr;
  return calloc(num_members, member_size);
}

void* Realloc(void* ptr, size_t num_members, size_t member_size) {
  FX_SAFE_SIZE_T total = num_members;
  total *= member_size;
  if (!total.IsValid() || total.ValueOrDie() >= kMallocSizeLimit)
    return nullptr;
  return realloc(ptr, total.ValueOrDie());
}

void Dealloc(void* ptr) {
  free(ptr);
}

void* StringAlloc(size_t num_members, size_t member_size) {
  FX_SAFE_SIZE_T total = member_size;
  total *= num_members;
  if (!total.IsValid())
    return nullptr;
  return malloc(total.ValueOrDie());
}

void StringDealloc(void* ptr) {
  free(ptr);
}

}  // namespace internal
}  // namespace pdfium

void FX_InitializeMemoryAllocators() {}

void FX_DestroyMemoryAllocators() {}

void* FX_ArrayBufferAllocate(size_t length) {
  void* result = calloc(length, 1);
  if (!result)
    FX_OutOfMemoryTerminate(length);
  return result;
}

void* FX_ArrayBufferAllocateUninitialized(size_t length) {
  void* result = malloc(length);
  if (!result)
    FX_OutOfMemoryTerminate(length);
  return result;
}

void FX_ArrayBufferFree(void* data) {
  free(data);
}

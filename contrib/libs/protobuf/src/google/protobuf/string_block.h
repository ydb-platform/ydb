// Protocol Buffers - Google's data interchange format
// Copyright 2023 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// This file defines the internal StringBlock class

#ifndef GOOGLE_PROTOBUF_STRING_BLOCK_H__
#define GOOGLE_PROTOBUF_STRING_BLOCK_H__

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>

#include "y_absl/base/attributes.h"
#include "y_absl/log/absl_check.h"
#include "google/protobuf/arena_align.h"
#include "google/protobuf/port.h"

// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {
namespace internal {

// StringBlock provides heap allocated, dynamically sized blocks (mini arenas)
// for allocating TProtoStringType instances. StringBlocks are allocated through
// the `New` function, and must be freed using the `Delete` function.
// StringBlocks are automatically sized from 256B to 8KB depending on the
// `next` instance provided in the `New` function to keep the average maximum
// unused space limited to 25%, or up to 4KB.
class alignas(TProtoStringType) StringBlock {
 public:
  StringBlock() = delete;
  StringBlock(const StringBlock&) = delete;
  StringBlock& operator=(const StringBlock&) = delete;

  // Allocates a new StringBlock pointing to `next`, which can be null.
  // The size of the returned block depends on the allocated size of `next`.
  static StringBlock* New(StringBlock* next);

  // Deletes `block`. `block` must not be null.
  static size_t Delete(StringBlock* block);

  StringBlock* next() const;

  // Returns the string instance at offset `offset`.
  // `offset` must be a multiple of sizeof(TProtoStringType), and be less than or
  // equal to `effective_size()`. `AtOffset(effective_size())` returns the
  // end of the allocated string instances and must not be de-referenced.
  Y_ABSL_ATTRIBUTE_RETURNS_NONNULL TProtoStringType* AtOffset(size_t offset);

  // Returns a pointer to the first string instance in this block.
  Y_ABSL_ATTRIBUTE_RETURNS_NONNULL TProtoStringType* begin();

  // Returns a pointer directly beyond the last string instance in this block.
  Y_ABSL_ATTRIBUTE_RETURNS_NONNULL TProtoStringType* end();

  // Returns the total allocation size of this instance.
  size_t allocated_size() const { return allocated_size_; }

  // Returns the effective size available for allocation string instances.
  // This value is guaranteed to be a multiple of sizeof(TProtoStringType), and
  // guaranteed to never be zero.
  size_t effective_size() const;

 private:
  static_assert(alignof(TProtoStringType) <= sizeof(void*), "");
  static_assert(alignof(TProtoStringType) <= ArenaAlignDefault::align, "");

  ~StringBlock() = default;

  explicit StringBlock(StringBlock* next, arc_ui32 size,
                       arc_ui32 next_size) noexcept
      : next_(next), allocated_size_(size), next_size_(next_size) {}

  static constexpr arc_ui32 min_size() { return size_t{256}; }
  static constexpr arc_ui32 max_size() { return size_t{8192}; }

  // Returns the size of the next block.
  size_t next_size() const { return next_size_; }

  StringBlock* const next_;
  const arc_ui32 allocated_size_;
  const arc_ui32 next_size_;
};

inline StringBlock* StringBlock::New(StringBlock* next) {
  // Compute required size, rounding down to a multiple of sizeof(std:string)
  // so that we can optimize the allocation path. I.e., we incur a (constant
  // size) MOD() operation cost here to avoid any MUL() later on.
  arc_ui32 size = min_size();
  arc_ui32 next_size = min_size();
  if (next) {
    size = next->next_size_;
    next_size = std::min(size * 2, max_size());
  }
  size -= (size - sizeof(StringBlock)) % sizeof(TProtoStringType);
  void* p = ::operator new(size);
  return new (p) StringBlock(next, size, next_size);
}

inline size_t StringBlock::Delete(StringBlock* block) {
  Y_ABSL_DCHECK(block != nullptr);
  size_t size = block->allocated_size();
  internal::SizedDelete(block, size);
  return size;
}

inline StringBlock* StringBlock::next() const { return next_; }

inline size_t StringBlock::effective_size() const {
  return allocated_size_ - sizeof(StringBlock);
}

Y_ABSL_ATTRIBUTE_RETURNS_NONNULL inline TProtoStringType* StringBlock::AtOffset(
    size_t offset) {
  Y_ABSL_DCHECK_LE(offset, effective_size());
  return reinterpret_cast<TProtoStringType*>(reinterpret_cast<char*>(this + 1) +
                                        offset);
}

Y_ABSL_ATTRIBUTE_RETURNS_NONNULL inline TProtoStringType* StringBlock::begin() {
  return AtOffset(0);
}

Y_ABSL_ATTRIBUTE_RETURNS_NONNULL inline TProtoStringType* StringBlock::end() {
  return AtOffset(effective_size());
}

}  // namespace internal
}  // namespace protobuf
}  // namespace google

#include "google/protobuf/port_undef.inc"

#endif  // GOOGLE_PROTOBUF_STRING_BLOCK_H__

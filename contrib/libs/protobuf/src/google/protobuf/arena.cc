// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
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

#include "google/protobuf/arena.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <typeinfo>

#include "y_absl/base/attributes.h"
#include "y_absl/container/internal/btree.h"
#include "y_absl/synchronization/mutex.h"
#include "google/protobuf/arena_allocation_policy.h"
#include "google/protobuf/arenaz_sampler.h"
#include "google/protobuf/port.h"
#include "google/protobuf/serial_arena.h"
#include "google/protobuf/thread_safe_arena.h"


#ifdef ADDRESS_SANITIZER
#include <sanitizer/asan_interface.h>
#endif  // ADDRESS_SANITIZER

// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {
namespace internal {
namespace {

#if defined(__GNUC__) && __GNUC__ >= 5
// kSentryArenaBlock is used for arenas which can be referenced pre-main. So,
// constexpr is required.
constexpr ArenaBlock kSentryArenaBlock;

ArenaBlock* SentryArenaBlock() {
  // const_cast<> is okay as kSentryArenaBlock will never be mutated.
  return const_cast<ArenaBlock*>(&kSentryArenaBlock);
}
#else
// TODO(b/248322260) Remove this once we're not using GCC 4.9 for tests.
// There is a compiler bug in this version that causes the above constexpr to
// fail.  This version is no longer in our support window, but we use it in
// some of our aarch64 docker images.
ArenaBlock* SentryArenaBlock() {
  static const ArenaBlock kSentryArenaBlock;
  // const_cast<> is okay as kSentryArenaBlock will never be mutated.
  return const_cast<ArenaBlock*>(&kSentryArenaBlock);
}
#endif

}  // namespace

static SizedPtr AllocateMemory(const AllocationPolicy* policy_ptr,
                               size_t last_size, size_t min_bytes) {
  AllocationPolicy policy;  // default policy
  if (policy_ptr) policy = *policy_ptr;
  size_t size;
  if (last_size != 0) {
    // Double the current block size, up to a limit.
    auto max_size = policy.max_block_size;
    size = std::min(2 * last_size, max_size);
  } else {
    size = policy.start_block_size;
  }
  // Verify that min_bytes + kBlockHeaderSize won't overflow.
  Y_ABSL_CHECK_LE(min_bytes, std::numeric_limits<size_t>::max() -
                               SerialArena::kBlockHeaderSize);
  size = std::max(size, SerialArena::kBlockHeaderSize + min_bytes);

  if (policy.block_alloc == nullptr) {
    return AllocateAtLeast(size);
  }
  return {policy.block_alloc(size), size};
}

class GetDeallocator {
 public:
  GetDeallocator(const AllocationPolicy* policy, size_t* space_allocated)
      : dealloc_(policy ? policy->block_dealloc : nullptr),
        space_allocated_(space_allocated) {}

  void operator()(SizedPtr mem) const {
#ifdef ADDRESS_SANITIZER
    // This memory was provided by the underlying allocator as unpoisoned,
    // so return it in an unpoisoned state.
    ASAN_UNPOISON_MEMORY_REGION(mem.p, mem.n);
#endif  // ADDRESS_SANITIZER
    if (dealloc_) {
      dealloc_(mem.p, mem.n);
    } else {
      internal::SizedDelete(mem.p, mem.n);
    }
    *space_allocated_ += mem.n;
  }

 private:
  void (*dealloc_)(void*, size_t);
  size_t* space_allocated_;
};

// It is guaranteed that this is constructed in `b`. IOW, this is not the first
// arena and `b` cannot be sentry.
SerialArena::SerialArena(ArenaBlock* b, ThreadSafeArena& parent)
    : ptr_{b->Pointer(kBlockHeaderSize + ThreadSafeArena::kSerialArenaSize)},
      limit_{b->Limit()},
      head_{b},
      space_allocated_{b->size},
      parent_{parent} {
  Y_ABSL_DCHECK(!b->IsSentry());
}

// It is guaranteed that this is the first SerialArena. Use sentry block.
SerialArena::SerialArena(ThreadSafeArena& parent)
    : head_{SentryArenaBlock()}, parent_{parent} {}

// It is guaranteed that this is the first SerialArena but `b` may be user
// provided or newly allocated to store AllocationPolicy.
SerialArena::SerialArena(FirstSerialArena, ArenaBlock* b,
                         ThreadSafeArena& parent)
    : head_{b}, space_allocated_{b->size}, parent_{parent} {
  if (b->IsSentry()) return;

  set_ptr(b->Pointer(kBlockHeaderSize));
  limit_ = b->Limit();
}

void SerialArena::Init(ArenaBlock* b, size_t offset) {
  set_ptr(b->Pointer(offset));
  limit_ = b->Limit();
  head_.store(b, std::memory_order_relaxed);
  space_used_.store(0, std::memory_order_relaxed);
  space_allocated_.store(b->size, std::memory_order_relaxed);
  cached_block_length_ = 0;
  cached_blocks_ = nullptr;
  string_block_ = nullptr;
  string_block_unused_.store(0, std::memory_order_relaxed);
}

SerialArena* SerialArena::New(SizedPtr mem, ThreadSafeArena& parent) {
  Y_ABSL_DCHECK_LE(kBlockHeaderSize + ThreadSafeArena::kSerialArenaSize, mem.n);
  ThreadSafeArenaStats::RecordAllocateStats(parent.arena_stats_.MutableStats(),
                                            /*used=*/0, /*allocated=*/mem.n,
                                            /*wasted=*/0);
  auto b = new (mem.p) ArenaBlock{nullptr, mem.n};
  return new (b->Pointer(kBlockHeaderSize)) SerialArena(b, parent);
}

template <typename Deallocator>
SizedPtr SerialArena::Free(Deallocator deallocator) {
  ArenaBlock* b = head();
  SizedPtr mem = {b, b->size};
  while (b->next) {
    b = b->next;  // We must first advance before deleting this block
    deallocator(mem);
    mem = {b, b->size};
  }
  return mem;
}

PROTOBUF_NOINLINE
void* SerialArena::AllocateAlignedFallback(size_t n) {
  AllocateNewBlock(n);
  return AllocateFromExisting(n);
}

PROTOBUF_NOINLINE
void* SerialArena::AllocateFromStringBlockFallback() {
  if (string_block_) {
    Y_ABSL_DCHECK_EQ(string_block_unused_.load(std::memory_order_relaxed), 0U);
    space_used_.store(space_used_.load(std::memory_order_relaxed) +
                          string_block_->effective_size(),
                      std::memory_order_relaxed);
  }

  string_block_ = StringBlock::New(string_block_);
  space_allocated_.store(space_allocated_.load(std::memory_order_relaxed) +
                             string_block_->allocated_size(),
                         std::memory_order_relaxed);

  size_t unused = string_block_->effective_size() - sizeof(TProtoStringType);
  string_block_unused_.store(unused, std::memory_order_relaxed);
  return string_block_->AtOffset(unused);
}

PROTOBUF_NOINLINE
void* SerialArena::AllocateAlignedWithCleanupFallback(
    size_t n, size_t align, void (*destructor)(void*)) {
  size_t required = AlignUpTo(n, align) + cleanup::Size(destructor);
  AllocateNewBlock(required);
  return AllocateFromExistingWithCleanupFallback(n, align, destructor);
}

PROTOBUF_NOINLINE
void SerialArena::AddCleanupFallback(void* elem, void (*destructor)(void*)) {
  size_t required = cleanup::Size(destructor);
  AllocateNewBlock(required);
  AddCleanupFromExisting(elem, destructor);
}

void SerialArena::AllocateNewBlock(size_t n) {
  size_t used = 0;
  size_t wasted = 0;
  ArenaBlock* old_head = head();
  if (!old_head->IsSentry()) {
    // Sync limit to block
    old_head->cleanup_nodes = limit_;

    // Record how much used in this block.
    used = static_cast<size_t>(ptr() - old_head->Pointer(kBlockHeaderSize));
    wasted = old_head->size - used;
    space_used_.store(space_used_.load(std::memory_order_relaxed) + used,
                      std::memory_order_relaxed);
  }

  // TODO(sbenza): Evaluate if pushing unused space into the cached blocks is a
  // win. In preliminary testing showed increased memory savings as expected,
  // but with a CPU regression. The regression might have been an artifact of
  // the microbenchmark.

  auto mem = AllocateMemory(parent_.AllocPolicy(), old_head->size, n);
  // We don't want to emit an expensive RMW instruction that requires
  // exclusive access to a cacheline. Hence we write it in terms of a
  // regular add.
  space_allocated_.store(
      space_allocated_.load(std::memory_order_relaxed) + mem.n,
      std::memory_order_relaxed);
  ThreadSafeArenaStats::RecordAllocateStats(parent_.arena_stats_.MutableStats(),
                                            /*used=*/used,
                                            /*allocated=*/mem.n, wasted);
  auto* new_head = new (mem.p) ArenaBlock{old_head, mem.n};
  set_ptr(new_head->Pointer(kBlockHeaderSize));
  limit_ = new_head->Limit();
  // Previous writes must take effect before writing new head.
  head_.store(new_head, std::memory_order_release);

#ifdef ADDRESS_SANITIZER
  ASAN_POISON_MEMORY_REGION(ptr(), limit_ - ptr());
#endif  // ADDRESS_SANITIZER
}

arc_ui64 SerialArena::SpaceUsed() const {
  // Note: the calculation below technically causes a race with
  // AllocateNewBlock when called from another thread (which happens in
  // ThreadSafeArena::SpaceUsed).  However, worst-case space_used_ will have
  // stale data and the calculation will incorrectly assume 100%
  // usage of the *current* block.
  // TODO(mkruskal) Consider eliminating this race in exchange for a possible
  // performance hit on ARM (see cl/455186837).
  arc_ui64 current_space_used =
      string_block_ ? string_block_->effective_size() -
                          string_block_unused_.load(std::memory_order_relaxed)
                    : 0;
  const ArenaBlock* h = head_.load(std::memory_order_acquire);
  if (h->IsSentry()) return current_space_used;

  const arc_ui64 current_block_size = h->size;
  current_space_used += std::min(
      static_cast<arc_ui64>(
          ptr() - const_cast<ArenaBlock*>(h)->Pointer(kBlockHeaderSize)),
      current_block_size);
  return current_space_used + space_used_.load(std::memory_order_relaxed);
}

size_t SerialArena::FreeStringBlocks(StringBlock* string_block,
                                     size_t unused_bytes) {
  Y_ABSL_DCHECK(string_block != nullptr);
  StringBlock* next = string_block->next();
  TProtoStringType* end = string_block->end();
  for (TProtoStringType* s = string_block->AtOffset(unused_bytes); s != end; ++s) {
    s->~TBasicString();
  }
  size_t deallocated = StringBlock::Delete(string_block);

  while ((string_block = next) != nullptr) {
    next = string_block->next();
    for (TProtoStringType& s : *string_block) {
      s.~TBasicString();
    }
    deallocated += StringBlock::Delete(string_block);
  }
  return deallocated;
}

void SerialArena::CleanupList() {
  ArenaBlock* b = head();
  if (b->IsSentry()) return;

  b->cleanup_nodes = limit_;
  do {
    char* limit = b->Limit();
    char* it = reinterpret_cast<char*>(b->cleanup_nodes);
    Y_ABSL_DCHECK(!b->IsSentry() || it == limit);
    while (it < limit) {
      it += cleanup::DestroyNode(it);
    }
    b = b->next;
  } while (b);
}

// Stores arrays of void* and SerialArena* instead of linked list of
// SerialArena* to speed up traversing all SerialArena. The cost of walk is non
// trivial when there are many nodes. Separately storing "ids" minimizes cache
// footprints and more efficient when looking for matching arena.
//
// Uses y_absl::container_internal::Layout to emulate the following:
//
// struct SerialArenaChunk {
//   struct SerialArenaChunkHeader {
//     SerialArenaChunk* next_chunk;
//     arc_ui32 capacity;
//     std::atomic<arc_ui32> size;
//   } header;
//   std::atomic<void*> ids[];
//   std::atomic<SerialArena*> arenas[];
// };
//
// where the size of "ids" and "arenas" is determined at runtime; hence the use
// of Layout.
struct SerialArenaChunkHeader {
  constexpr SerialArenaChunkHeader(arc_ui32 capacity, arc_ui32 size)
      : next_chunk(nullptr), capacity(capacity), size(size) {}

  ThreadSafeArena::SerialArenaChunk* next_chunk;
  arc_ui32 capacity;
  std::atomic<arc_ui32> size;
};

class ThreadSafeArena::SerialArenaChunk {
 public:
  SerialArenaChunk(arc_ui32 capacity, void* me, SerialArena* serial) {
    new (&header()) SerialArenaChunkHeader{capacity, 1};

    new (&id(0)) std::atomic<void*>{me};
    for (arc_ui32 i = 1; i < capacity; ++i) {
      new (&id(i)) std::atomic<void*>{nullptr};
    }

    new (&arena(0)) std::atomic<SerialArena*>{serial};
    for (arc_ui32 i = 1; i < capacity; ++i) {
      new (&arena(i)) std::atomic<void*>{nullptr};
    }
  }

  bool IsSentry() const { return capacity() == 0; }

  // next_chunk
  const SerialArenaChunk* next_chunk() const { return header().next_chunk; }
  SerialArenaChunk* next_chunk() { return header().next_chunk; }
  void set_next(SerialArenaChunk* next_chunk) {
    header().next_chunk = next_chunk;
  }

  // capacity
  arc_ui32 capacity() const { return header().capacity; }
  void set_capacity(arc_ui32 capacity) { header().capacity = capacity; }

  // ids: returns up to size().
  y_absl::Span<const std::atomic<void*>> ids() const {
    return Layout(capacity()).Slice<kIds>(ptr()).first(safe_size());
  }
  y_absl::Span<std::atomic<void*>> ids() {
    return Layout(capacity()).Slice<kIds>(ptr()).first(safe_size());
  }
  std::atomic<void*>& id(arc_ui32 i) {
    Y_ABSL_DCHECK_LT(i, capacity());
    return Layout(capacity()).Pointer<kIds>(ptr())[i];
  }

  // arenas: returns up to size().
  y_absl::Span<const std::atomic<SerialArena*>> arenas() const {
    return Layout(capacity()).Slice<kArenas>(ptr()).first(safe_size());
  }
  y_absl::Span<std::atomic<SerialArena*>> arenas() {
    return Layout(capacity()).Slice<kArenas>(ptr()).first(safe_size());
  }
  const std::atomic<SerialArena*>& arena(arc_ui32 i) const {
    Y_ABSL_DCHECK_LT(i, capacity());
    return Layout(capacity()).Pointer<kArenas>(ptr())[i];
  }
  std::atomic<SerialArena*>& arena(arc_ui32 i) {
    Y_ABSL_DCHECK_LT(i, capacity());
    return Layout(capacity()).Pointer<kArenas>(ptr())[i];
  }

  // Tries to insert {id, serial} to head chunk. Returns false if the head is
  // already full.
  //
  // Note that the updating "size", "id", "arena" is individually atomic but
  // those are not protected by a mutex. This is acceptable because concurrent
  // lookups from SpaceUsed or SpaceAllocated accept inaccuracy due to race. On
  // other paths, either race is not possible (GetSerialArenaFallback) or must
  // be prevented by users (CleanupList, Free).
  bool insert(void* me, SerialArena* serial) {
    arc_ui32 idx = size().fetch_add(1, std::memory_order_relaxed);
    // Bail out if this chunk is full.
    if (idx >= capacity()) {
      // Write old value back to avoid potential overflow.
      size().store(capacity(), std::memory_order_relaxed);
      return false;
    }

    id(idx).store(me, std::memory_order_relaxed);
    arena(idx).store(serial, std::memory_order_release);
    return true;
  }

  constexpr static size_t AllocSize(size_t n) { return Layout(n).AllocSize(); }

 private:
  constexpr static int kHeader = 0;
  constexpr static int kIds = 1;
  constexpr static int kArenas = 2;

  using layout_type = y_absl::container_internal::Layout<
      SerialArenaChunkHeader, std::atomic<void*>, std::atomic<SerialArena*>>;

  const char* ptr() const { return reinterpret_cast<const char*>(this); }
  char* ptr() { return reinterpret_cast<char*>(this); }

  SerialArenaChunkHeader& header() {
    return *layout_type::Partial().Pointer<kHeader>(ptr());
  }
  const SerialArenaChunkHeader& header() const {
    return *layout_type::Partial().Pointer<kHeader>(ptr());
  }

  std::atomic<arc_ui32>& size() { return header().size; }
  const std::atomic<arc_ui32>& size() const { return header().size; }

  // Returns the size capped by the capacity as fetch_add may result in a size
  // greater than capacity.
  arc_ui32 safe_size() const {
    return std::min(capacity(), size().load(std::memory_order_relaxed));
  }

  constexpr static layout_type Layout(size_t n) {
    return layout_type(
        /*header*/ 1,
        /*ids*/ n,
        /*arenas*/ n);
  }
};

constexpr SerialArenaChunkHeader kSentryArenaChunk = {0, 0};

ThreadSafeArena::SerialArenaChunk* ThreadSafeArena::SentrySerialArenaChunk() {
  // const_cast is okay because the sentry chunk is never mutated. Also,
  // reinterpret_cast is acceptable here as it should be identical to
  // SerialArenaChunk with zero payload. This is a necessary trick to
  // constexpr initialize kSentryArenaChunk.
  return reinterpret_cast<SerialArenaChunk*>(
      const_cast<SerialArenaChunkHeader*>(&kSentryArenaChunk));
}


alignas(kCacheAlignment) Y_ABSL_CONST_INIT
    std::atomic<ThreadSafeArena::LifecycleId> ThreadSafeArena::lifecycle_id_{0};
#if defined(PROTOBUF_NO_THREADLOCAL)
ThreadSafeArena::ThreadCache& ThreadSafeArena::thread_cache() {
  static internal::ThreadLocalStorage<ThreadCache>* thread_cache_ =
      new internal::ThreadLocalStorage<ThreadCache>();
  return *thread_cache_->Get();
}
#elif defined(PROTOBUF_USE_DLLS)
ThreadSafeArena::ThreadCache& ThreadSafeArena::thread_cache() {
  static PROTOBUF_THREAD_LOCAL ThreadCache thread_cache;
  return thread_cache;
}
#else
PROTOBUF_CONSTINIT PROTOBUF_THREAD_LOCAL
    ThreadSafeArena::ThreadCache ThreadSafeArena::thread_cache_;
#endif

ThreadSafeArena::ThreadSafeArena() : first_arena_(*this) { Init(); }

ThreadSafeArena::ThreadSafeArena(char* mem, size_t size)
    : first_arena_(FirstSerialArena{}, FirstBlock(mem, size), *this) {
  Init();
}

ThreadSafeArena::ThreadSafeArena(void* mem, size_t size,
                                 const AllocationPolicy& policy)
    : first_arena_(FirstSerialArena{}, FirstBlock(mem, size, policy), *this) {
  InitializeWithPolicy(policy);
}

ArenaBlock* ThreadSafeArena::FirstBlock(void* buf, size_t size) {
  Y_ABSL_DCHECK_EQ(reinterpret_cast<uintptr_t>(buf) & 7, 0u);
  if (buf == nullptr || size <= kBlockHeaderSize) {
    return SentryArenaBlock();
  }
  // Record user-owned block.
  alloc_policy_.set_is_user_owned_initial_block(true);
  return new (buf) ArenaBlock{nullptr, size};
}

ArenaBlock* ThreadSafeArena::FirstBlock(void* buf, size_t size,
                                        const AllocationPolicy& policy) {
  if (policy.IsDefault()) return FirstBlock(buf, size);

  Y_ABSL_DCHECK_EQ(reinterpret_cast<uintptr_t>(buf) & 7, 0u);

  SizedPtr mem;
  if (buf == nullptr || size < kBlockHeaderSize + kAllocPolicySize) {
    mem = AllocateMemory(&policy, 0, kAllocPolicySize);
  } else {
    mem = {buf, size};
    // Record user-owned block.
    alloc_policy_.set_is_user_owned_initial_block(true);
  }

  return new (mem.p) ArenaBlock{nullptr, mem.n};
}

void ThreadSafeArena::InitializeWithPolicy(const AllocationPolicy& policy) {
  Init();

  if (policy.IsDefault()) return;

#ifndef NDEBUG
  const arc_ui64 old_alloc_policy = alloc_policy_.get_raw();
  // If there was a policy (e.g., in Reset()), make sure flags were preserved.
#define Y_ABSL_DCHECK_POLICY_FLAGS_() \
  if (old_alloc_policy > 3)         \
  Y_ABSL_CHECK_EQ(old_alloc_policy & 3, alloc_policy_.get_raw() & 3)
#else
#define Y_ABSL_DCHECK_POLICY_FLAGS_()
#endif  // NDEBUG

  // We ensured enough space so this cannot fail.
  void* p;
  if (!first_arena_.MaybeAllocateAligned(kAllocPolicySize, &p)) {
    Y_ABSL_LOG(FATAL) << "MaybeAllocateAligned cannot fail here.";
    return;
  }
  new (p) AllocationPolicy{policy};
  // Low bits store flags, so they mustn't be overwritten.
  Y_ABSL_DCHECK_EQ(0u, reinterpret_cast<uintptr_t>(p) & 3);
  alloc_policy_.set_policy(reinterpret_cast<AllocationPolicy*>(p));
  Y_ABSL_DCHECK_POLICY_FLAGS_();

#undef Y_ABSL_DCHECK_POLICY_FLAGS_
}

arc_ui64 ThreadSafeArena::GetNextLifeCycleId() {
  ThreadCache& tc = thread_cache();
  arc_ui64 id = tc.next_lifecycle_id;
  constexpr arc_ui64 kInc = ThreadCache::kPerThreadIds;
  if (PROTOBUF_PREDICT_FALSE((id & (kInc - 1)) == 0)) {
    // On platforms that don't support arc_ui64 atomics we can certainly not
    // afford to increment by large intervals and expect uniqueness due to
    // wrapping, hence we only add by 1.
    id = lifecycle_id_.fetch_add(1, std::memory_order_relaxed) * kInc;
  }
  tc.next_lifecycle_id = id + 1;
  return id;
}

// We assume that #threads / arena is bimodal; i.e. majority small ones are
// single threaded but some big ones are highly concurrent. To balance between
// memory overhead and minimum pointer chasing, we start with few entries and
// exponentially (4x) grow with a limit (255 entries). Note that parameters are
// picked for x64 architectures as hint and the actual size is calculated by
// Layout.
ThreadSafeArena::SerialArenaChunk* ThreadSafeArena::NewSerialArenaChunk(
    arc_ui32 prev_capacity, void* id, SerialArena* serial) {
  constexpr size_t kMaxBytes = 4096;  // Can hold up to 255 entries.
  constexpr size_t kGrowthFactor = 4;
  constexpr size_t kHeaderSize = SerialArenaChunk::AllocSize(0);
  constexpr size_t kEntrySize = SerialArenaChunk::AllocSize(1) - kHeaderSize;

  // On x64 arch: {4, 16, 64, 256, 256, ...} * 16.
  size_t prev_bytes = SerialArenaChunk::AllocSize(prev_capacity);
  size_t next_bytes = std::min(kMaxBytes, prev_bytes * kGrowthFactor);
  arc_ui32 next_capacity =
      static_cast<arc_ui32>(next_bytes - kHeaderSize) / kEntrySize;
  // Growth based on bytes needs to be adjusted by AllocSize.
  next_bytes = SerialArenaChunk::AllocSize(next_capacity);

  // If we allocate bigger memory than requested, we should expand
  // size to use that extra space, and add extra entries permitted
  // by the extra space.
  SizedPtr mem = AllocateAtLeast(next_bytes);
  next_capacity = static_cast<arc_ui32>(mem.n - kHeaderSize) / kEntrySize;
  Y_ABSL_DCHECK_LE(SerialArenaChunk::AllocSize(next_capacity), mem.n);
  return new (mem.p) SerialArenaChunk{next_capacity, id, serial};
}

// Tries to reserve an entry by atomic fetch_add. If the head chunk is already
// full (size >= capacity), acquires the mutex and adds a new head.
void ThreadSafeArena::AddSerialArena(void* id, SerialArena* serial) {
  SerialArenaChunk* head = head_.load(std::memory_order_acquire);
  // Fast path without acquiring mutex.
  if (!head->IsSentry() && head->insert(id, serial)) {
    return;
  }

  // Slow path with acquiring mutex.
  y_absl::MutexLock lock(&mutex_);

  // Refetch and if someone else installed a new head, try allocating on that!
  SerialArenaChunk* new_head = head_.load(std::memory_order_acquire);
  if (new_head != head) {
    if (new_head->insert(id, serial)) return;
    // Update head to link to the latest one.
    head = new_head;
  }

  new_head = NewSerialArenaChunk(head->capacity(), id, serial);
  new_head->set_next(head);

  // Use "std::memory_order_release" to make sure prior stores are visible after
  // this one.
  head_.store(new_head, std::memory_order_release);
}

void ThreadSafeArena::Init() {
  tag_and_id_ = GetNextLifeCycleId();
  arena_stats_ = Sample();
  head_.store(SentrySerialArenaChunk(), std::memory_order_relaxed);
  first_owner_ = &thread_cache();

  // Record allocation for the first block that was either user-provided or
  // newly allocated.
  ThreadSafeArenaStats::RecordAllocateStats(
      arena_stats_.MutableStats(),
      /*used=*/0,
      /*allocated=*/first_arena_.SpaceAllocated(),
      /*wasted=*/0);

  CacheSerialArena(&first_arena_);
}

ThreadSafeArena::~ThreadSafeArena() {
  // Have to do this in a first pass, because some of the destructors might
  // refer to memory in other blocks.
  CleanupList();

  size_t space_allocated = 0;
  auto mem = Free(&space_allocated);
  if (alloc_policy_.is_user_owned_initial_block()) {
#ifdef ADDRESS_SANITIZER
    // Unpoison the initial block, now that it's going back to the user.
    ASAN_UNPOISON_MEMORY_REGION(mem.p, mem.n);
#endif  // ADDRESS_SANITIZER
    space_allocated += mem.n;
  } else if (mem.n > 0) {
    GetDeallocator(alloc_policy_.get(), &space_allocated)(mem);
  }
}

SizedPtr ThreadSafeArena::Free(size_t* space_allocated) {
  auto deallocator = GetDeallocator(alloc_policy_.get(), space_allocated);

  WalkSerialArenaChunk([&](SerialArenaChunk* chunk) {
    y_absl::Span<std::atomic<SerialArena*>> span = chunk->arenas();
    // Walks arenas backward to handle the first serial arena the last. Freeing
    // in reverse-order to the order in which objects were created may not be
    // necessary to Free and we should revisit this. (b/247560530)
    for (auto it = span.rbegin(); it != span.rend(); ++it) {
      SerialArena* serial = it->load(std::memory_order_relaxed);
      Y_ABSL_DCHECK_NE(serial, nullptr);
      // Free string blocks
      *space_allocated += serial->FreeStringBlocks();
      // Always frees the first block of "serial" as it cannot be user-provided.
      SizedPtr mem = serial->Free(deallocator);
      Y_ABSL_DCHECK_NE(mem.p, nullptr);
      deallocator(mem);
    }

    // Delete the chunk as we're done with it.
    internal::SizedDelete(chunk,
                          SerialArenaChunk::AllocSize(chunk->capacity()));
  });

  // The first block of the first arena is special and let the caller handle it.
  *space_allocated += first_arena_.FreeStringBlocks();
  return first_arena_.Free(deallocator);
}

arc_ui64 ThreadSafeArena::Reset() {
  // Have to do this in a first pass, because some of the destructors might
  // refer to memory in other blocks.
  CleanupList();

  // Discard all blocks except the first one. Whether it is user-provided or
  // allocated, always reuse the first block for the first arena.
  size_t space_allocated = 0;
  auto mem = Free(&space_allocated);
  space_allocated += mem.n;

  // Reset the first arena with the first block. This avoids redundant
  // free / allocation and re-allocating for AllocationPolicy. Adjust offset if
  // we need to preserve alloc_policy_.
  if (alloc_policy_.is_user_owned_initial_block() ||
      alloc_policy_.get() != nullptr) {
    size_t offset = alloc_policy_.get() == nullptr
                        ? kBlockHeaderSize
                        : kBlockHeaderSize + kAllocPolicySize;
    first_arena_.Init(new (mem.p) ArenaBlock{nullptr, mem.n}, offset);
  } else {
    first_arena_.Init(SentryArenaBlock(), 0);
  }

  // Since the first block and potential alloc_policy on the first block is
  // preserved, this can be initialized by Init().
  Init();

  return space_allocated;
}

void* ThreadSafeArena::AllocateAlignedWithCleanup(size_t n, size_t align,
                                                  void (*destructor)(void*)) {
  SerialArena* arena;
  if (PROTOBUF_PREDICT_TRUE(GetSerialArenaFast(&arena))) {
    return arena->AllocateAlignedWithCleanup(n, align, destructor);
  } else {
    return AllocateAlignedWithCleanupFallback(n, align, destructor);
  }
}

void ThreadSafeArena::AddCleanup(void* elem, void (*cleanup)(void*)) {
  SerialArena* arena;
  if (PROTOBUF_PREDICT_FALSE(!GetSerialArenaFast(&arena))) {
    arena = GetSerialArenaFallback(kMaxCleanupNodeSize);
  }
  arena->AddCleanup(elem, cleanup);
}

PROTOBUF_NOINLINE
void* ThreadSafeArena::AllocateAlignedWithCleanupFallback(
    size_t n, size_t align, void (*destructor)(void*)) {
  return GetSerialArenaFallback(n + kMaxCleanupNodeSize)
      ->AllocateAlignedWithCleanup(n, align, destructor);
}

PROTOBUF_NOINLINE
void* ThreadSafeArena::AllocateFromStringBlock() {
  SerialArena* arena;
  if (PROTOBUF_PREDICT_FALSE(!GetSerialArenaFast(&arena))) {
    arena = GetSerialArenaFallback(0);
  }
  return arena->AllocateFromStringBlock();
}

template <typename Functor>
void ThreadSafeArena::WalkConstSerialArenaChunk(Functor fn) const {
  const SerialArenaChunk* chunk = head_.load(std::memory_order_acquire);

  for (; !chunk->IsSentry(); chunk = chunk->next_chunk()) {
    fn(chunk);
  }
}

template <typename Functor>
void ThreadSafeArena::WalkSerialArenaChunk(Functor fn) {
  // By omitting an Acquire barrier we help the sanitizer that any user code
  // that doesn't properly synchronize Reset() or the destructor will throw a
  // TSAN warning.
  SerialArenaChunk* chunk = head_.load(std::memory_order_relaxed);

  while (!chunk->IsSentry()) {
    // Cache next chunk in case this chunk is destroyed.
    SerialArenaChunk* next_chunk = chunk->next_chunk();
    fn(chunk);
    chunk = next_chunk;
  }
}

template <typename Functor>
void ThreadSafeArena::PerConstSerialArenaInChunk(Functor fn) const {
  WalkConstSerialArenaChunk([&fn](const SerialArenaChunk* chunk) {
    for (const auto& each : chunk->arenas()) {
      const SerialArena* serial = each.load(std::memory_order_acquire);
      // It is possible that newly added SerialArena is not updated although
      // size was. This is acceptable for SpaceAllocated and SpaceUsed.
      if (serial == nullptr) continue;
      fn(serial);
    }
  });
}

arc_ui64 ThreadSafeArena::SpaceAllocated() const {
  arc_ui64 space_allocated = first_arena_.SpaceAllocated();
  PerConstSerialArenaInChunk([&space_allocated](const SerialArena* serial) {
    space_allocated += serial->SpaceAllocated();
  });
  return space_allocated;
}

arc_ui64 ThreadSafeArena::SpaceUsed() const {
  // First arena is inlined to ThreadSafeArena and the first block's overhead is
  // smaller than others that contain SerialArena.
  arc_ui64 space_used = first_arena_.SpaceUsed();
  PerConstSerialArenaInChunk([&space_used](const SerialArena* serial) {
    // SerialArena on chunks directly allocated from the block and needs to be
    // subtracted from SpaceUsed.
    space_used += serial->SpaceUsed() - kSerialArenaSize;
  });
  return space_used - (alloc_policy_.get() ? sizeof(AllocationPolicy) : 0);
}

template <AllocationClient alloc_client>
PROTOBUF_NOINLINE void* ThreadSafeArena::AllocateAlignedFallback(size_t n) {
  return GetSerialArenaFallback(n)->AllocateAligned<alloc_client>(n);
}

template void* ThreadSafeArena::AllocateAlignedFallback<
    AllocationClient::kDefault>(size_t);
template void*
    ThreadSafeArena::AllocateAlignedFallback<AllocationClient::kArray>(size_t);

void ThreadSafeArena::CleanupList() {
  WalkSerialArenaChunk([](SerialArenaChunk* chunk) {
    y_absl::Span<std::atomic<SerialArena*>> span = chunk->arenas();
    // Walks arenas backward to handle the first serial arena the last.
    // Destroying in reverse-order to the construction is often assumed by users
    // and required not to break inter-object dependencies. (b/247560530)
    for (auto it = span.rbegin(); it != span.rend(); ++it) {
      SerialArena* serial = it->load(std::memory_order_relaxed);
      Y_ABSL_DCHECK_NE(serial, nullptr);
      serial->CleanupList();
    }
  });
  // First arena must be cleaned up last. (b/247560530)
  first_arena_.CleanupList();
}

PROTOBUF_NOINLINE
SerialArena* ThreadSafeArena::GetSerialArenaFallback(size_t n) {
  void* const id = &thread_cache();
  if (id == first_owner_) {
    CacheSerialArena(&first_arena_);
    return &first_arena_;
  }

  // Search matching SerialArena.
  SerialArena* serial = nullptr;
  WalkConstSerialArenaChunk([&serial, id](const SerialArenaChunk* chunk) {
    y_absl::Span<const std::atomic<void*>> ids = chunk->ids();
    for (arc_ui32 i = 0; i < ids.size(); ++i) {
      if (ids[i].load(std::memory_order_relaxed) == id) {
        serial = chunk->arena(i).load(std::memory_order_relaxed);
        Y_ABSL_DCHECK_NE(serial, nullptr);
        break;
      }
    }
  });

  if (!serial) {
    // This thread doesn't have any SerialArena, which also means it doesn't
    // have any blocks yet.  So we'll allocate its first block now. It must be
    // big enough to host SerialArena and the pending request.
    serial = SerialArena::New(
        AllocateMemory(alloc_policy_.get(), 0, n + kSerialArenaSize), *this);

    AddSerialArena(id, serial);
  }

  CacheSerialArena(serial);
  return serial;
}

}  // namespace internal

void* Arena::Allocate(size_t n) { return impl_.AllocateAligned(n); }

void* Arena::AllocateForArray(size_t n) {
  return impl_.AllocateAligned<internal::AllocationClient::kArray>(n);
}

void* Arena::AllocateAlignedWithCleanup(size_t n, size_t align,
                                        void (*destructor)(void*)) {
  return impl_.AllocateAlignedWithCleanup(n, align, destructor);
}

}  // namespace protobuf
}  // namespace google

#include "google/protobuf/port_undef.inc"

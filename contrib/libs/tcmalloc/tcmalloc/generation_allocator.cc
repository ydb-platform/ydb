#include "tcmalloc/generation_allocator.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <new>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "absl/base/call_once.h"
#include "absl/base/internal/low_level_scheduling.h"
#include "absl/base/internal/spinlock.h"

namespace tcmalloc::tcmalloc_internal {
namespace {
using Lock = absl::base_internal::SpinLockHolder;
size_t Log2(size_t value) { return 63 - __builtin_clzll(value); }
uint64_t MonotonicNow() {
  timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) abort();
  return uint64_t(now.tv_sec) * 1000000000 + now.tv_nsec;
}
size_t Setting(const char* name, size_t fallback) {
  const char* text = getenv(name);
  if (!text) return fallback;
  size_t value = 0;
  if (!*text) abort();
  for (; *text; ++text) {
    if (*text < '0' || *text > '9' ||
        value > (std::numeric_limits<size_t>::max() - (*text - '0')) / 10) {
      abort();
    }
    value = value * 10 + (*text - '0');
  }
  return value;
}
}  // namespace

void GenerationAllocator::Init(const Config& config, BackingAllocator backing,
                               Clock clock) {
  config_ = config;
  backing_ = backing;
  clock_ = clock;
}

size_t GenerationAllocator::FirstOffset(size_t alignment) {
  return (sizeof(Header) + alignment - 1) & ~(alignment - 1);
}

uintptr_t GenerationAllocator::Checksum(const Header& h) {
  return uintptr_t{0x72913bd5a4c6e807ULL} ^ reinterpret_cast<uintptr_t>(&h) ^
      reinterpret_cast<uintptr_t>(h.next) ^ h.deadline ^ h.requested ^
      h.offset ^ static_cast<uintptr_t>(h.state);
}
void GenerationAllocator::Seal(Header* h) { h->checksum = Checksum(*h); }
void GenerationAllocator::Check(const Header& h) {
  if (h.checksum != Checksum(h)) Fail("CORRUPTED_HEADER", &h, nullptr, 0);
}

[[noreturn]] void GenerationAllocator::Fail(const char* reason, const void* ptr,
                                           const Header* h, size_t slot_size) {
  // Fixed buffer, no allocator hooks or stack unwinder on the fatal path.
  char buffer[512];
  size_t count = 0;
  auto text = [&](const char* value) {
    while (*value && count < sizeof(buffer)) buffer[count++] = *value++;
  };
  auto number = [&](uintptr_t value, unsigned base) {
    char digits[32];
    size_t n = 0;
    do {
      digits[n++] = "0123456789abcdef"[value % base];
      value /= base;
    } while (value);
    while (n && count < sizeof(buffer)) buffer[count++] = digits[--n];
  };
  text("TCMalloc generation: "); text(reason);
  text(" pointer=0x"); number(reinterpret_cast<uintptr_t>(ptr), 16);
  text(" base=0x"); number(reinterpret_cast<uintptr_t>(h), 16);
  text(" expected=0x"); number(h ? reinterpret_cast<uintptr_t>(h) + h->offset : 0, 16);
  text(" slot_size="); number(slot_size, 10);
  text(" offset="); number(h ? h->offset : 0, 10);
  text(" requested="); number(h ? h->requested : 0, 10);
  text(" state="); number(h ? static_cast<uintptr_t>(h->state) : 0, 10);
  text("\n");
  size_t remaining = count;
  const char* cursor = buffer;
  while (remaining) {
    const ssize_t written = write(STDERR_FILENO, cursor, remaining);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) break;
    cursor += written;
    remaining -= written;
  }
  abort();
}

GenerationAllocator::Region* GenerationAllocator::Find(const void* ptr) const {
  const uintptr_t page = reinterpret_cast<uintptr_t>(ptr) / kGranule;
  for (Entry* e = registry_[page % kBuckets].load(std::memory_order_acquire);
       e; e = e->next) {
    if (e->page == page) return e->region;
  }
  return nullptr;
}
bool GenerationAllocator::Owns(const void* ptr) const { return Find(ptr) != nullptr; }

bool GenerationAllocator::Populate(Pool* pool, size_t slot_size, size_t alignment) {
  Lock population(&population_lock_);
  {
    Lock lock(&pool->lock);
    if (pool->free) return true;
  }
  const size_t length = std::max(slot_size, kGranule);
  // HPAA only accepts alignment up to one huge page. Keep any alignment
  // padding permanently reserved and charge it to the same budget.
  constexpr size_t kBackingAlignmentLimit = 2 * 1024 * 1024;
  const size_t backing_alignment = std::min(length, kBackingAlignmentLimit);
  const size_t backing_length = length + (length - backing_alignment);
  const size_t count = length / kGranule;
  const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
  if (count > (std::numeric_limits<size_t>::max() - sizeof(Region) - page_size) /
                  sizeof(Entry)) return false;
  const size_t metadata = (sizeof(Region) + count * sizeof(Entry) + page_size - 1) & ~(page_size - 1);
  if (backing_length > config_.budget || metadata > config_.budget - backing_length) return false;
  const size_t charge = backing_length + metadata;
  size_t used = reserved_.load(std::memory_order_relaxed);
  do {
    if (used > config_.budget - charge) return false;
  } while (!reserved_.compare_exchange_weak(used, used + charge,
                                            std::memory_order_relaxed));
  void* storage = mmap(nullptr, metadata, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (storage == MAP_FAILED) {
    reserved_.fetch_sub(charge, std::memory_order_relaxed);
    return false;
  }
  void* memory = backing_(backing_length, backing_alignment);
  if (!memory) {
    munmap(storage, metadata);
    reserved_.fetch_sub(charge, std::memory_order_relaxed);
    return false;
  }
  auto* region = new (storage) Region{
      (reinterpret_cast<uintptr_t>(memory) + length - 1) & ~(length - 1),
      length, slot_size, alignment, pool};
  auto* entries = reinterpret_cast<Entry*>(region + 1);
  // Initialize headers before publishing the immutable registry entries.
  Header* first = nullptr;
  Header* last = nullptr;
  for (size_t offset = 0; offset < length; offset += slot_size) {
    auto* h = new (reinterpret_cast<void*>(region->begin + offset)) Header{
        first, 0, 0, FirstOffset(alignment), State::Free, 0};
    Seal(h);
    if (!last) last = h;
    first = h;
  }
  for (size_t i = 0; i < count; ++i) {
    const uintptr_t page = region->begin / kGranule + i;
    auto& bucket = registry_[page % kBuckets];
    auto* e = new (&entries[i]) Entry{page, region, bucket.load(std::memory_order_relaxed)};
    while (!bucket.compare_exchange_weak(e->next, e, std::memory_order_release,
                                         std::memory_order_relaxed)) {}
  }
  Lock lock(&pool->lock);
  last->next = pool->free;
  Seal(last);
  pool->free = first;
  return true;
}

GenerationAllocator::Header* GenerationAllocator::Validate(
    const void* ptr, const Region& region) {
  auto* h = reinterpret_cast<Header*>(reinterpret_cast<uintptr_t>(ptr) &
                                      ~(region.slot_size - 1));
  Check(*h);
  if (h->state == State::Free) Fail("DOUBLE_FREE", ptr, h, region.slot_size);
  if (h->state == State::Quarantined)
    Fail("FREE_DURING_QUARANTINE", ptr, h, region.slot_size);
  if (reinterpret_cast<uintptr_t>(ptr) != reinterpret_cast<uintptr_t>(h) + h->offset)
    Fail("STALE_GENERATION_FREE", ptr, h, region.slot_size);
  return h;
}

void GenerationAllocator::Poison(Header* h, size_t slot_size, bool verify) {
  // Only the first/last 64 bytes of the last requested area, not metadata.
  auto* bytes = reinterpret_cast<volatile unsigned char*>(h) + h->offset;
  const size_t n = std::min(h->requested, size_t{64});
  for (size_t i = 0; i < n; ++i) {
    const size_t offsets[] = {i, h->requested - 1 - i};
    for (size_t offset : offsets) {
      const unsigned char expected = static_cast<unsigned char>(
          reinterpret_cast<uintptr_t>(h) >> 8 ^ offset ^ 0xa5);
      if (verify) {
        if (bytes[offset] != expected) Fail("WRITE_AFTER_FREE", const_cast<const unsigned char*>(bytes + offset), h, slot_size);
      } else {
        bytes[offset] = expected;
      }
    }
  }
}

void GenerationAllocator::Drain(Pool* pool, size_t slot_size) {
  if (!pool->head) return;
  const uint64_t now = clock_();
  for (size_t i = 0; i < 8 && pool->head; ++i) {
    Header* h = pool->head;
    Check(*h);
    if (now < h->deadline) break;
    if (config_.poison) Poison(h, slot_size, true);
    pool->head = h->next;
    if (!pool->head) pool->tail = nullptr;
    h->next = pool->free;
    h->state = State::Free;
    // Recover fixed geometry from immutable registry, never from old request.
    h->offset = FirstOffset(Find(h)->alignment);
    h->deadline = 0;
    h->requested = 0;
    Seal(h);
    pool->free = h;
    pool->quarantined -= slot_size;
  }
}

size_t GenerationAllocator::Capacity(size_t size, size_t alignment) {
  if (alignment == 0 || (alignment & (alignment - 1))) return 0;
  alignment = std::max(alignment, sizeof(uintptr_t));
  if (size > size_t{1} << 61 || alignment > size_t{1} << 60) return 0;
  size_t slot_size = 256;
  const size_t needed = std::max({size * 2, alignment * 4, sizeof(Header) * 4});
  while (slot_size < needed) slot_size *= 2;
  return slot_size / 2;
}

GenerationAllocator::Result GenerationAllocator::Allocate(
    size_t size, size_t alignment, size_t shard) {
  // Fixed capacity and alignment prevent early address reuse as requests vary.
  const size_t capacity = Capacity(size, alignment);
  if (!capacity) return {nullptr, 0};
  alignment = std::max(alignment, sizeof(uintptr_t));
  const size_t slot_size = capacity * 2;
  Pool* pool = &pools_[Log2(slot_size)][Log2(alignment)][shard % kShards];
  for (;;) {
    {
      Lock lock(&pool->lock);
      Drain(pool, slot_size);
      if (Header* h = pool->free) {
        Check(*h);
        if (h->state != State::Free) Fail("CORRUPTED_HEADER", h, h, slot_size);
        pool->free = h->next;
        h->next = nullptr;
        h->state = State::Allocated;
        h->requested = size;
        Seal(h);
        pool->live += size;
        return {reinterpret_cast<char*>(h) + h->offset, slot_size / 2};
      }
    }
    if (!Populate(pool, slot_size, alignment)) return {nullptr, 0};
  }
}

void GenerationAllocator::Free(void* ptr) {
  if (!ptr) return;
  Region* region = Find(ptr);
  if (!region) Fail("INVALID_SLOT_POINTER", ptr, nullptr, 0);
  Pool* pool = region->pool;
  Lock lock(&pool->lock);
  Header* h = Validate(ptr, *region);
  pool->live -= h->requested;
  if (h->offset + region->alignment <= region->slot_size / 2) {
    h->offset += region->alignment;
    h->state = State::Free;
    h->next = pool->free;
    pool->free = h;
  } else {
    if (config_.poison) Poison(h, region->slot_size, false);
    const uint64_t now = clock_();
    h->deadline = config_.quarantine_ns > UINT64_MAX - now ? UINT64_MAX :
                  now + config_.quarantine_ns;
    h->state = State::Quarantined;
    h->next = nullptr;
    if (pool->tail) {
      Check(*pool->tail);
      pool->tail->next = h;
      Seal(pool->tail);
    } else {
      pool->head = h;
    }
    pool->tail = h;
    pool->quarantined += region->slot_size;
  }
  Seal(h);
}

size_t GenerationAllocator::Size(const void* ptr) {
  if (!ptr) return 0;
  Region* region = Find(ptr);
  if (!region) Fail("INVALID_SLOT_POINTER", ptr, nullptr, 0);
  Lock lock(&region->pool->lock);
  Validate(ptr, *region);
  return region->slot_size / 2;
}
GenerationAllocator::Stats GenerationAllocator::GetStats() {
  size_t live = 0;
  size_t quarantined = 0;
  for (auto& sizes : pools_) for (auto& aligns : sizes) {
    for (auto& pool : aligns) {
      Lock lock(&pool.lock);
      live += pool.live;
      quarantined += pool.quarantined;
    }
  }
  return {reserved_.load(std::memory_order_relaxed), live,
          quarantined};
}

void GenerationAllocator::LockAll() {
  population_lock_.Lock();
  for (auto& sizes : pools_) for (auto& aligns : sizes)
    for (auto& pool : aligns) pool.lock.Lock();
}
void GenerationAllocator::UnlockAll() {
  for (auto& sizes : pools_) for (auto& aligns : sizes)
    for (auto& pool : aligns) pool.lock.Unlock();
  population_lock_.Unlock();
}

GenerationAllocator& GenerationGlobal() {
  ABSL_CONST_INIT static GenerationAllocator allocator;
  ABSL_CONST_INIT static absl::once_flag once;
  absl::base_internal::LowLevelCallOnce(&once, [&] {
    const size_t budget = Setting("TCMALLOC_GENERATION_BUDGET_BYTES", size_t{16} << 30);
    const size_t delay = Setting("TCMALLOC_GENERATION_QUARANTINE_MS", 1000);
    if (delay > UINT64_MAX / 1000000) abort();
    allocator.Init({budget, delay * 1000000,
                    Setting("TCMALLOC_GENERATION_POISON", 0) != 0},
                   GenerationBackingAllocate, MonotonicNow);
  });
  return allocator;
}
}  // namespace tcmalloc::tcmalloc_internal

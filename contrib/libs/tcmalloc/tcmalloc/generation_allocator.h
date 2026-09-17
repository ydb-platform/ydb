// Diagnostic allocator: persistent slot headers and offset generations.
#ifndef TCMALLOC_GENERATION_ALLOCATOR_H_
#define TCMALLOC_GENERATION_ALLOCATOR_H_

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "absl/base/const_init.h"
#include "absl/base/internal/spinlock.h"

namespace tcmalloc::tcmalloc_internal {

class GenerationAllocator {
 public:
  using BackingAllocator = void* (*)(size_t size, size_t alignment);
  using Clock = uint64_t (*)();  // Monotonic nanoseconds; must not allocate.
  struct Config {
    size_t budget;
    uint64_t quarantine_ns;
    bool poison;
  };
  struct Result { void* ptr; size_t capacity; };
  struct Stats { size_t reserved; size_t live; size_t quarantined; };

  constexpr GenerationAllocator() = default;
  // Before first use only. All backing and registry memory is process-lifetime.
  void Init(const Config& config, BackingAllocator backing, Clock clock);
  static size_t Capacity(size_t size, size_t alignment);
  Result Allocate(size_t size, size_t alignment, size_t shard);
  void Free(void* ptr);
  size_t Size(const void* ptr);
  bool Owns(const void* ptr) const;
  Stats GetStats();
  void LockAll();
  void UnlockAll();

 private:
  enum class State : uintptr_t { Free, Allocated, Quarantined };
  struct Header {
    Header* next;
    uint64_t deadline;
    size_t requested;
    size_t offset;
    State state;
    uintptr_t checksum;
  };
  struct alignas(64) Pool {
    absl::base_internal::SpinLock lock{
        absl::base_internal::SCHEDULE_KERNEL_ONLY};
    Header* free = nullptr;
    Header* head = nullptr;
    Header* tail = nullptr;
    size_t live = 0;
    size_t quarantined = 0;
  };
  struct Region {
    uintptr_t begin;
    size_t length;
    size_t slot_size;
    size_t alignment;
    Pool* pool;
  };
  struct Entry {
    uintptr_t page;
    Region* region;
    Entry* next;
  };
  static constexpr size_t kGranule = 256 * 1024;
  static constexpr size_t kBuckets = 65536;
  static constexpr size_t kShards = 8;
  absl::base_internal::SpinLock population_lock_{
      absl::base_internal::SCHEDULE_KERNEL_ONLY};
  Pool pools_[64][64][kShards]{};
  std::atomic<Entry*> registry_[kBuckets]{};
  std::atomic<size_t> reserved_{0};
  Config config_{};
  BackingAllocator backing_ = nullptr;
  Clock clock_ = nullptr;

  Region* Find(const void* ptr) const;
  bool Populate(Pool* pool, size_t slot_size, size_t alignment);
  Header* Validate(const void* ptr, const Region& region);
  static uintptr_t Checksum(const Header& header);
  static void Seal(Header* header);
  static void Check(const Header& header);
  static size_t FirstOffset(size_t alignment);
  static void Poison(Header* header, size_t slot_size, bool verify);
  void Drain(Pool* pool, size_t slot_size);
  [[noreturn]] static void Fail(const char* reason, const void* ptr,
                              const Header* header, size_t slot_size);
};

GenerationAllocator& GenerationGlobal();
void* GenerationBackingAllocate(size_t size, size_t alignment);

}  // namespace tcmalloc::tcmalloc_internal
#endif

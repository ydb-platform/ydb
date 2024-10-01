// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/memory_pool.h"

#include <algorithm>  // IWYU pragma: keep
#include <atomic>
#include <cstdlib>   // IWYU pragma: keep
#include <cstring>   // IWYU pragma: keep
#include <iostream>  // IWYU pragma: keep
#include <limits>
#include <memory>

#if defined(sun) || defined(__sun)
#include <stdlib.h>
#endif

#include "arrow/buffer.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"  // IWYU pragma: keep
#include "arrow/util/optional.h"
#include "arrow/util/string.h"
#include "arrow/util/thread_pool.h"

#ifdef __GLIBC__
#include <malloc.h>
#endif

#ifdef ARROW_JEMALLOC
// Needed to support jemalloc 3 and 4
#define JEMALLOC_MANGLE
// Explicitly link to our version of jemalloc
#error #include "jemalloc_ep/dist/include/jemalloc/jemalloc.h"
#endif

#ifdef ARROW_MIMALLOC
#error #include <mimalloc.h>
#endif

#ifdef ARROW_JEMALLOC

// Compile-time configuration for jemalloc options.
// Note the prefix ("je_arrow_") must match the symbol prefix given when
// building jemalloc.
// See discussion in https://github.com/jemalloc/jemalloc/issues/1621

// ARROW-6910(wesm): we found that jemalloc's default behavior with respect to
// dirty / muzzy pages (see definitions of these in the jemalloc documentation)
// conflicted with user expectations, and would even cause memory use problems
// in some cases. By enabling the background_thread option and reducing the
// decay time from 10 seconds to 1 seconds, memory is released more
// aggressively (and in the background) to the OS. This can be configured
// further by using the arrow::jemalloc_set_decay_ms API

#undef USE_JEMALLOC_BACKGROUND_THREAD
#ifndef __APPLE__
// ARROW-6977: jemalloc's background_thread isn't always enabled on macOS
#define USE_JEMALLOC_BACKGROUND_THREAD
#endif

// In debug mode, add memory poisoning on alloc / free
#ifdef NDEBUG
#define JEMALLOC_DEBUG_OPTIONS ""
#else
#define JEMALLOC_DEBUG_OPTIONS ",junk:true"
#endif

const char* je_arrow_malloc_conf =
    ("oversize_threshold:0"
#ifdef USE_JEMALLOC_BACKGROUND_THREAD
     ",dirty_decay_ms:1000"
     ",muzzy_decay_ms:1000"
     ",background_thread:true"
#else
     // ARROW-6994: return memory immediately to the OS if the
     // background_thread option isn't available
     ",dirty_decay_ms:0"
     ",muzzy_decay_ms:0"
#endif
     JEMALLOC_DEBUG_OPTIONS);  // NOLINT: whitespace/parens

#endif  // ARROW_JEMALLOC

namespace arrow {

namespace {

constexpr size_t kAlignment = 64;

constexpr char kDefaultBackendEnvVar[] = "ARROW_DEFAULT_MEMORY_POOL";

enum class MemoryPoolBackend : uint8_t { System, Jemalloc, Mimalloc };

struct SupportedBackend {
  const char* name;
  MemoryPoolBackend backend;
};

// See ARROW-12248 for why we use static in-function singletons rather than
// global constants below (in SupportedBackends() and UserSelectedBackend()).
// In some contexts (especially R bindings) `default_memory_pool()` may be
// called before all globals are initialized, and then the ARROW_DEFAULT_MEMORY_POOL
// environment variable would be ignored.

const std::vector<SupportedBackend>& SupportedBackends() {
  static std::vector<SupportedBackend> backends = {
  // ARROW-12316: Apple => mimalloc first, then jemalloc
  //              non-Apple => jemalloc first, then mimalloc
#if defined(ARROW_JEMALLOC) && !defined(__APPLE__)
    {"jemalloc", MemoryPoolBackend::Jemalloc},
#endif
#ifdef ARROW_MIMALLOC
    {"mimalloc", MemoryPoolBackend::Mimalloc},
#endif
#if defined(ARROW_JEMALLOC) && defined(__APPLE__)
    {"jemalloc", MemoryPoolBackend::Jemalloc},
#endif
    {"system", MemoryPoolBackend::System}
  };
  return backends;
}

// Return the MemoryPoolBackend selected by the user through the
// ARROW_DEFAULT_MEMORY_POOL environment variable, if any.
util::optional<MemoryPoolBackend> UserSelectedBackend() {
  static auto user_selected_backend = []() -> util::optional<MemoryPoolBackend> {
    auto unsupported_backend = [](const std::string& name) {
      std::vector<std::string> supported;
      for (const auto backend : SupportedBackends()) {
        supported.push_back(std::string("'") + backend.name + "'");
      }
      ARROW_LOG(WARNING) << "Unsupported backend '" << name << "' specified in "
                         << kDefaultBackendEnvVar << " (supported backends are "
                         << internal::JoinStrings(supported, ", ") << ")";
    };

    auto maybe_name = internal::GetEnvVar(kDefaultBackendEnvVar);
    if (!maybe_name.ok()) {
      return {};
    }
    const auto name = *std::move(maybe_name);
    if (name.empty()) {
      // An empty environment variable is considered missing
      return {};
    }
    const auto found = std::find_if(
        SupportedBackends().begin(), SupportedBackends().end(),
        [&](const SupportedBackend& backend) { return name == backend.name; });
    if (found != SupportedBackends().end()) {
      return found->backend;
    }
    unsupported_backend(name);
    return {};
  }();

  return user_selected_backend;
}

MemoryPoolBackend DefaultBackend() {
  auto backend = UserSelectedBackend();
  if (backend.has_value()) {
    return backend.value();
  }
  struct SupportedBackend default_backend = SupportedBackends().front();
  return default_backend.backend;
}

// A static piece of memory for 0-size allocations, so as to return
// an aligned non-null pointer.
alignas(kAlignment) static uint8_t zero_size_area[1];

// Helper class directing allocations to the standard system allocator.
class SystemAllocator {
 public:
  // Allocate memory according to the alignment requirements for Arrow
  // (as of May 2016 64 bytes)
  static Status AllocateAligned(int64_t size, uint8_t** out) {
    if (size == 0) {
      *out = zero_size_area;
      return Status::OK();
    }
#ifdef _WIN32
    // Special code path for Windows
    *out = reinterpret_cast<uint8_t*>(
        _aligned_malloc(static_cast<size_t>(size), kAlignment));
    if (!*out) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
#elif defined(sun) || defined(__sun)
    *out = reinterpret_cast<uint8_t*>(memalign(kAlignment, static_cast<size_t>(size)));
    if (!*out) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
#else
    const int result = posix_memalign(reinterpret_cast<void**>(out), kAlignment,
                                      static_cast<size_t>(size));
    if (result == ENOMEM) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }

    if (result == EINVAL) {
      return Status::Invalid("invalid alignment parameter: ", kAlignment);
    }
#endif
    return Status::OK();
  }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == zero_size_area) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size);
      *ptr = zero_size_area;
      return Status::OK();
    }
    // Note: We cannot use realloc() here as it doesn't guarantee alignment.

    // Allocate new chunk
    uint8_t* out = nullptr;
    RETURN_NOT_OK(AllocateAligned(new_size, &out));
    DCHECK(out);
    // Copy contents and release old memory chunk
    memcpy(out, *ptr, static_cast<size_t>(std::min(new_size, old_size)));
#ifdef _WIN32
    _aligned_free(*ptr);
#else
    free(*ptr);
#endif  // defined(_WIN32)
    *ptr = out;
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size) {
    if (ptr == zero_size_area) {
      DCHECK_EQ(size, 0);
    } else {
#ifdef _WIN32
      _aligned_free(ptr);
#else
      free(ptr);
#endif
    }
  }

  static void ReleaseUnused() {
#ifdef __GLIBC__
    // The return value of malloc_trim is not an error but to inform
    // you if memory was actually released or not, which we do not care about here
    ARROW_UNUSED(malloc_trim(0));
#endif
  }
};

#ifdef ARROW_JEMALLOC

// Helper class directing allocations to the jemalloc allocator.
class JemallocAllocator {
 public:
  static Status AllocateAligned(int64_t size, uint8_t** out) {
    if (size == 0) {
      *out = zero_size_area;
      return Status::OK();
    }
    *out = reinterpret_cast<uint8_t*>(
        mallocx(static_cast<size_t>(size), MALLOCX_ALIGN(kAlignment)));
    if (*out == NULL) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    return Status::OK();
  }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == zero_size_area) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size);
      *ptr = zero_size_area;
      return Status::OK();
    }
    *ptr = reinterpret_cast<uint8_t*>(
        rallocx(*ptr, static_cast<size_t>(new_size), MALLOCX_ALIGN(kAlignment)));
    if (*ptr == NULL) {
      *ptr = previous_ptr;
      return Status::OutOfMemory("realloc of size ", new_size, " failed");
    }
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size) {
    if (ptr == zero_size_area) {
      DCHECK_EQ(size, 0);
    } else {
      dallocx(ptr, MALLOCX_ALIGN(kAlignment));
    }
  }

  static void ReleaseUnused() {
    mallctl("arena." ARROW_STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", NULL, NULL, NULL, 0);
  }
};

#endif  // defined(ARROW_JEMALLOC)

#ifdef ARROW_MIMALLOC

// Helper class directing allocations to the mimalloc allocator.
class MimallocAllocator {
 public:
  static Status AllocateAligned(int64_t size, uint8_t** out) {
    if (size == 0) {
      *out = zero_size_area;
      return Status::OK();
    }
    *out = reinterpret_cast<uint8_t*>(
        mi_malloc_aligned(static_cast<size_t>(size), kAlignment));
    if (*out == NULL) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    return Status::OK();
  }

  static void ReleaseUnused() { mi_collect(true); }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == zero_size_area) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size);
      *ptr = zero_size_area;
      return Status::OK();
    }
    *ptr = reinterpret_cast<uint8_t*>(
        mi_realloc_aligned(previous_ptr, static_cast<size_t>(new_size), kAlignment));
    if (*ptr == NULL) {
      *ptr = previous_ptr;
      return Status::OutOfMemory("realloc of size ", new_size, " failed");
    }
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size) {
    if (ptr == zero_size_area) {
      DCHECK_EQ(size, 0);
    } else {
      mi_free(ptr);
    }
  }
};

#endif  // defined(ARROW_MIMALLOC)

}  // namespace

int64_t MemoryPool::max_memory() const { return -1; }

///////////////////////////////////////////////////////////////////////
// MemoryPool implementation that delegates its core duty
// to an Allocator class.

#ifndef NDEBUG
static constexpr uint8_t kAllocPoison = 0xBC;
static constexpr uint8_t kReallocPoison = 0xBD;
static constexpr uint8_t kDeallocPoison = 0xBE;
#endif

template <typename Allocator>
class BaseMemoryPoolImpl : public MemoryPool {
 public:
  ~BaseMemoryPoolImpl() override {}

  Status Allocate(int64_t size, uint8_t** out) override {
    if (size < 0) {
      return Status::Invalid("negative malloc size");
    }
    if (static_cast<uint64_t>(size) >= std::numeric_limits<size_t>::max()) {
      return Status::CapacityError("malloc size overflows size_t");
    }
    RETURN_NOT_OK(Allocator::AllocateAligned(size, out));
#ifndef NDEBUG
    // Poison data
    if (size > 0) {
      DCHECK_NE(*out, nullptr);
      (*out)[0] = kAllocPoison;
      (*out)[size - 1] = kAllocPoison;
    }
#endif

    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    if (new_size < 0) {
      return Status::Invalid("negative realloc size");
    }
    if (static_cast<uint64_t>(new_size) >= std::numeric_limits<size_t>::max()) {
      return Status::CapacityError("realloc overflows size_t");
    }
    RETURN_NOT_OK(Allocator::ReallocateAligned(old_size, new_size, ptr));
#ifndef NDEBUG
    // Poison data
    if (new_size > old_size) {
      DCHECK_NE(*ptr, nullptr);
      (*ptr)[old_size] = kReallocPoison;
      (*ptr)[new_size - 1] = kReallocPoison;
    }
#endif

    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) override {
#ifndef NDEBUG
    // Poison data
    if (size > 0) {
      DCHECK_NE(buffer, nullptr);
      buffer[0] = kDeallocPoison;
      buffer[size - 1] = kDeallocPoison;
    }
#endif
    Allocator::DeallocateAligned(buffer, size);

    stats_.UpdateAllocatedBytes(-size);
  }

  void ReleaseUnused() override { Allocator::ReleaseUnused(); }

  int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

  int64_t max_memory() const override { return stats_.max_memory(); }

 protected:
  internal::MemoryPoolStats stats_;
};

class SystemMemoryPool : public BaseMemoryPoolImpl<SystemAllocator> {
 public:
  std::string backend_name() const override { return "system"; }
};

#ifdef ARROW_JEMALLOC
class JemallocMemoryPool : public BaseMemoryPoolImpl<JemallocAllocator> {
 public:
  std::string backend_name() const override { return "jemalloc"; }
};
#endif

#ifdef ARROW_MIMALLOC
class MimallocMemoryPool : public BaseMemoryPoolImpl<MimallocAllocator> {
 public:
  std::string backend_name() const override { return "mimalloc"; }
};
#endif

std::unique_ptr<MemoryPool> MemoryPool::CreateDefault() {
  auto backend = DefaultBackend();
  switch (backend) {
    case MemoryPoolBackend::System:
      return std::unique_ptr<MemoryPool>(new SystemMemoryPool);
#ifdef ARROW_JEMALLOC
    case MemoryPoolBackend::Jemalloc:
      return std::unique_ptr<MemoryPool>(new JemallocMemoryPool);
#endif
#ifdef ARROW_MIMALLOC
    case MemoryPoolBackend::Mimalloc:
      return std::unique_ptr<MemoryPool>(new MimallocMemoryPool);
#endif
    default:
      ARROW_LOG(FATAL) << "Internal error: cannot create default memory pool";
      return nullptr;
  }
}

static struct GlobalState {
  ~GlobalState() { finalizing.store(true, std::memory_order_relaxed); }

  bool is_finalizing() const { return finalizing.load(std::memory_order_relaxed); }

  std::atomic<bool> finalizing{false};  // constructed first, destroyed last

  SystemMemoryPool system_pool;
#ifdef ARROW_JEMALLOC
  JemallocMemoryPool jemalloc_pool;
#endif
#ifdef ARROW_MIMALLOC
  MimallocMemoryPool mimalloc_pool;
#endif
} global_state;

MemoryPool* system_memory_pool() { return &global_state.system_pool; }

Status jemalloc_memory_pool(MemoryPool** out) {
#ifdef ARROW_JEMALLOC
  *out = &global_state.jemalloc_pool;
  return Status::OK();
#else
  return Status::NotImplemented("This Arrow build does not enable jemalloc");
#endif
}

Status mimalloc_memory_pool(MemoryPool** out) {
#ifdef ARROW_MIMALLOC
  *out = &global_state.mimalloc_pool;
  return Status::OK();
#else
  return Status::NotImplemented("This Arrow build does not enable mimalloc");
#endif
}

MemoryPool* __attribute__((weak)) default_memory_pool() {
  auto backend = DefaultBackend();
  switch (backend) {
    case MemoryPoolBackend::System:
      return &global_state.system_pool;
#ifdef ARROW_JEMALLOC
    case MemoryPoolBackend::Jemalloc:
      return &global_state.jemalloc_pool;
#endif
#ifdef ARROW_MIMALLOC
    case MemoryPoolBackend::Mimalloc:
      return &global_state.mimalloc_pool;
#endif
    default:
      ARROW_LOG(FATAL) << "Internal error: cannot create default memory pool";
      return nullptr;
  }
}

#define RETURN_IF_JEMALLOC_ERROR(ERR)                  \
  do {                                                 \
    if (err != 0) {                                    \
      return Status::UnknownError(std::strerror(ERR)); \
    }                                                  \
  } while (0)

Status jemalloc_set_decay_ms(int ms) {
#ifdef ARROW_JEMALLOC
  ssize_t decay_time_ms = static_cast<ssize_t>(ms);

  int err = mallctl("arenas.dirty_decay_ms", nullptr, nullptr, &decay_time_ms,
                    sizeof(decay_time_ms));
  RETURN_IF_JEMALLOC_ERROR(err);
  err = mallctl("arenas.muzzy_decay_ms", nullptr, nullptr, &decay_time_ms,
                sizeof(decay_time_ms));
  RETURN_IF_JEMALLOC_ERROR(err);

  return Status::OK();
#else
  return Status::Invalid("jemalloc support is not built");
#endif
}

///////////////////////////////////////////////////////////////////////
// LoggingMemoryPool implementation

LoggingMemoryPool::LoggingMemoryPool(MemoryPool* pool) : pool_(pool) {}

Status LoggingMemoryPool::Allocate(int64_t size, uint8_t** out) {
  Status s = pool_->Allocate(size, out);
  std::cout << "Allocate: size = " << size << std::endl;
  return s;
}

Status LoggingMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  Status s = pool_->Reallocate(old_size, new_size, ptr);
  std::cout << "Reallocate: old_size = " << old_size << " - new_size = " << new_size
            << std::endl;
  return s;
}

void LoggingMemoryPool::Free(uint8_t* buffer, int64_t size) {
  pool_->Free(buffer, size);
  std::cout << "Free: size = " << size << std::endl;
}

int64_t LoggingMemoryPool::bytes_allocated() const {
  int64_t nb_bytes = pool_->bytes_allocated();
  std::cout << "bytes_allocated: " << nb_bytes << std::endl;
  return nb_bytes;
}

int64_t LoggingMemoryPool::max_memory() const {
  int64_t mem = pool_->max_memory();
  std::cout << "max_memory: " << mem << std::endl;
  return mem;
}

std::string LoggingMemoryPool::backend_name() const { return pool_->backend_name(); }

///////////////////////////////////////////////////////////////////////
// ProxyMemoryPool implementation

class ProxyMemoryPool::ProxyMemoryPoolImpl {
 public:
  explicit ProxyMemoryPoolImpl(MemoryPool* pool) : pool_(pool) {}

  Status Allocate(int64_t size, uint8_t** out) {
    RETURN_NOT_OK(pool_->Allocate(size, out));
    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    RETURN_NOT_OK(pool_->Reallocate(old_size, new_size, ptr));
    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) {
    pool_->Free(buffer, size);
    stats_.UpdateAllocatedBytes(-size);
  }

  int64_t bytes_allocated() const { return stats_.bytes_allocated(); }

  int64_t max_memory() const { return stats_.max_memory(); }

  std::string backend_name() const { return pool_->backend_name(); }

 private:
  MemoryPool* pool_;
  internal::MemoryPoolStats stats_;
};

ProxyMemoryPool::ProxyMemoryPool(MemoryPool* pool) {
  impl_.reset(new ProxyMemoryPoolImpl(pool));
}

ProxyMemoryPool::~ProxyMemoryPool() {}

Status ProxyMemoryPool::Allocate(int64_t size, uint8_t** out) {
  return impl_->Allocate(size, out);
}

Status ProxyMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  return impl_->Reallocate(old_size, new_size, ptr);
}

void ProxyMemoryPool::Free(uint8_t* buffer, int64_t size) {
  return impl_->Free(buffer, size);
}

int64_t ProxyMemoryPool::bytes_allocated() const { return impl_->bytes_allocated(); }

int64_t ProxyMemoryPool::max_memory() const { return impl_->max_memory(); }

std::string ProxyMemoryPool::backend_name() const { return impl_->backend_name(); }

std::vector<std::string> SupportedMemoryBackendNames() {
  std::vector<std::string> supported;
  for (const auto backend : SupportedBackends()) {
    supported.push_back(backend.name);
  }
  return supported;
}

// -----------------------------------------------------------------------
// Pool buffer and allocation

/// A Buffer whose lifetime is tied to a particular MemoryPool
class PoolBuffer final : public ResizableBuffer {
 public:
  explicit PoolBuffer(std::shared_ptr<MemoryManager> mm, MemoryPool* pool)
      : ResizableBuffer(nullptr, 0, std::move(mm)), pool_(pool) {}

  ~PoolBuffer() override {
    // Avoid calling pool_->Free if the global pools are destroyed
    // (XXX this will not work with user-defined pools)

    // This can happen if a Future is destructing on one thread while or
    // after memory pools are destructed on the main thread (as there is
    // no guarantee of destructor order between thread/memory pools)
    uint8_t* ptr = mutable_data();
    if (ptr && !global_state.is_finalizing()) {
      pool_->Free(ptr, capacity_);
    }
  }

  Status Reserve(const int64_t capacity) override {
    if (capacity < 0) {
      return Status::Invalid("Negative buffer capacity: ", capacity);
    }
    uint8_t* ptr = mutable_data();
    if (!ptr || capacity > capacity_) {
      int64_t new_capacity = BitUtil::RoundUpToMultipleOf64(capacity);
      if (ptr) {
        RETURN_NOT_OK(pool_->Reallocate(capacity_, new_capacity, &ptr));
      } else {
        RETURN_NOT_OK(pool_->Allocate(new_capacity, &ptr));
      }
      data_ = ptr;
      capacity_ = new_capacity;
    }
    return Status::OK();
  }

  Status Resize(const int64_t new_size, bool shrink_to_fit = true) override {
    if (ARROW_PREDICT_FALSE(new_size < 0)) {
      return Status::Invalid("Negative buffer resize: ", new_size);
    }
    uint8_t* ptr = mutable_data();
    if (ptr && shrink_to_fit && new_size <= size_) {
      // Buffer is non-null and is not growing, so shrink to the requested size without
      // excess space.
      int64_t new_capacity = BitUtil::RoundUpToMultipleOf64(new_size);
      if (capacity_ != new_capacity) {
        // Buffer hasn't got yet the requested size.
        RETURN_NOT_OK(pool_->Reallocate(capacity_, new_capacity, &ptr));
        data_ = ptr;
        capacity_ = new_capacity;
      }
    } else {
      RETURN_NOT_OK(Reserve(new_size));
    }
    size_ = new_size;

    return Status::OK();
  }

  static std::shared_ptr<PoolBuffer> MakeShared(MemoryPool* pool) {
    std::shared_ptr<MemoryManager> mm;
    if (pool == nullptr) {
      pool = default_memory_pool();
      mm = default_cpu_memory_manager();
    } else {
      mm = CPUDevice::memory_manager(pool);
    }
    return std::make_shared<PoolBuffer>(std::move(mm), pool);
  }

  static std::unique_ptr<PoolBuffer> MakeUnique(MemoryPool* pool) {
    std::shared_ptr<MemoryManager> mm;
    if (pool == nullptr) {
      pool = default_memory_pool();
      mm = default_cpu_memory_manager();
    } else {
      mm = CPUDevice::memory_manager(pool);
    }
    return std::unique_ptr<PoolBuffer>(new PoolBuffer(std::move(mm), pool));
  }

 private:
  MemoryPool* pool_;
};

namespace {
// A utility that does most of the work of the `AllocateBuffer` and
// `AllocateResizableBuffer` methods. The argument `buffer` should be a smart pointer to
// a PoolBuffer.
template <typename BufferPtr, typename PoolBufferPtr>
inline Result<BufferPtr> ResizePoolBuffer(PoolBufferPtr&& buffer, const int64_t size) {
  RETURN_NOT_OK(buffer->Resize(size));
  buffer->ZeroPadding();
  return std::move(buffer);
}

}  // namespace

Result<std::unique_ptr<Buffer>> AllocateBuffer(const int64_t size, MemoryPool* pool) {
  return ResizePoolBuffer<std::unique_ptr<Buffer>>(PoolBuffer::MakeUnique(pool), size);
}

Result<std::unique_ptr<ResizableBuffer>> AllocateResizableBuffer(const int64_t size,
                                                                 MemoryPool* pool) {
  return ResizePoolBuffer<std::unique_ptr<ResizableBuffer>>(PoolBuffer::MakeUnique(pool),
                                                            size);
}

}  // namespace arrow

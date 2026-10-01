#pragma once

#include "dq_compute_actor_async_io.h"

#include <arrow/memory_pool.h>

#include <atomic>

namespace NYql::NDq {

// Arrow pool that charges its buffers to the memory quota manager bound to the allocating thread (see
// TArrowMemoryQuotaScope), e.g. the per query arrow quota manager of KQP. The memory is system malloc, never MKQL:
// the buffers are charged to the bound manager only. Every buffer keeps a reference to the manager it was charged to
// and returns the quota to it when freed, on any thread and after the scope is gone. Unbound allocations are not
// charged. The quota manager must be thread safe. A refused charge fails the allocation with Status::OutOfMemory, so
// the callers must check the statuses (no VERIFY) and report a memory limit error.
class TDqArrowMemoryPool final : public arrow::MemoryPool {
public:
    arrow::Status Allocate(int64_t size, uint8_t** out) override;
    arrow::Status Reallocate(int64_t oldSize, int64_t newSize, uint8_t** ptr) override;
    void Free(uint8_t* buffer, int64_t size) override;

    int64_t bytes_allocated() const override {
        return BytesAllocated.load();
    }

    int64_t max_memory() const override {
        return MaxMemory.load();
    }

    std::string backend_name() const override {
        return "DQ quota";
    }

private:
    void UpdateAllocatedBytes(int64_t diff);

    std::atomic<int64_t> BytesAllocated = 0;
    std::atomic<int64_t> MaxMemory = 0;
};

TDqArrowMemoryPool* GetDqArrowMemoryPool();

// The manager bound to the current thread, nullptr when unbound
IMemoryQuotaManager* GetArrowMemoryQuota();

// RAII binding of an arrow quota manager to the current thread, opened by the actor that allocates on behalf of a
// query (compute actor, source). Nestable: the destructor restores the outer binding. The scope holds a reference, so
// the manager outlives it. `quota` may be nullptr, which unbinds for the duration of the scope.
class TArrowMemoryQuotaScope {
public:
    explicit TArrowMemoryQuotaScope(IMemoryQuotaManager::TPtr quota);
    ~TArrowMemoryQuotaScope();

    TArrowMemoryQuotaScope(const TArrowMemoryQuotaScope&) = delete;
    TArrowMemoryQuotaScope& operator=(const TArrowMemoryQuotaScope&) = delete;

private:
    const IMemoryQuotaManager::TPtr Quota;
    const IMemoryQuotaManager::TPtr* const Prev;
};

// Process wide switch of the arrow memory quota, set once at startup (TableServiceConfig.ResourceManager
// .EnableArrowMemoryQuota) together with the default arrow allocator of MKQL
void SetArrowMemoryQuotaEnabled(bool enabled);
bool IsArrowMemoryQuotaEnabled();

} // namespace NYql::NDq

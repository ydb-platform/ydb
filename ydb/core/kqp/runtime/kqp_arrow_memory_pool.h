#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_arrow_memory_pool.h>

#include <util/generic/singleton.h>

#include <arrow/memory_pool.h>

namespace NKikimr::NMiniKQL {

class TArrowMemoryPool : public arrow::MemoryPool {
public:
    arrow::Status Allocate(int64_t size, uint8_t** out) final;
    arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) final;
    void Free(uint8_t* buffer, int64_t size) final;

    int64_t max_memory() const final {
        return MaxMemory_.load();
    }

    int64_t bytes_allocated() const final {
        return BytesAllocated_.load();
    }

    std::string backend_name() const final {
        return "MKQL";
    }

private:
    inline void UpdateAllocatedBytes(int64_t diff) {
        // inspired by arrow/memory_pool.h impl.
        int64_t allocated = BytesAllocated_.fetch_add(diff) + diff;
        if (diff > 0 && allocated > MaxMemory_) {
            MaxMemory_ = allocated;
        }
    }

private:
    std::atomic<int64_t> BytesAllocated_{0};
    std::atomic<int64_t> MaxMemory_{0};
};

// With the arrow memory quota on, the buffers are charged to the arrow quota manager bound to the thread, see
// NYql::NDq::TDqArrowMemoryPool: the callers must check the statuses, a refused charge is Status::OutOfMemory
static inline arrow::MemoryPool* GetArrowMemoryPool() {
    if (NYql::NDq::IsArrowMemoryQuotaEnabled()) {
        return NYql::NDq::GetDqArrowMemoryPool();
    }
    return Singleton<TArrowMemoryPool>();
}

} // namespace NKikimr::NMiniKQL

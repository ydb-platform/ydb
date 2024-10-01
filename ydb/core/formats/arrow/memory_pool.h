#pragma once

#ifndef USE_CURRENT_UDF_ABI_VERSION
#   define USE_CURRENT_UDF_ABI_VERSION
#endif

#include <ydb/library/yql/public/udf/udf_version.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/system/mutex.h>

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/table.h>

namespace NKikimr::NArrow {

class TMemoryPool : public arrow::MemoryPool {

    arrow::Status Allocate(int64_t size, uint8_t** out) final;
    arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) final;
    void Free(uint8_t* buffer, int64_t size) final;

    int64_t max_memory() const final {
        return MaxMemory_.load();
    }

    int64_t bytes_allocated() const final {
        return BytesAllocated_.load();
    }

    inline void UpdateAllocatedBytes(int64_t diff) {
        // inspired by arrow/memory_pool.h impl.
        int64_t allocated = BytesAllocated_.fetch_add(diff) + diff;
        if (diff > 0 && allocated > MaxMemory_) {
            MaxMemory_ = allocated;
        }
    }

    std::string backend_name() const final {
        return "MKQL";
    }

public:
    // Do auto-untrack within `Free()` - as the best effort in hope on proper TLSAllocState,
    // otherwise we'll have unused extra-tracked memory until AllocState destruction.
    // Manual untrack is complicated because one table may share buffers with other tables.
    void Track(const std::shared_ptr<arrow::Table>& table);

private:
    std::atomic<int64_t> BytesAllocated_{0};
    std::atomic<int64_t> MaxMemory_{0};
};

static inline TMemoryPool* GetMemoryPool() {
    return Singleton<TMemoryPool>();
}

} // namespace NKikimr::NArrow

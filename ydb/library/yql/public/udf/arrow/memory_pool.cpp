#include "memory_pool.h"

#include <ydb/library/yql/public/udf/udf_allocator.h>

#include <util/generic/singleton.h>
#include <util/generic/yexception.h>

namespace NYql {
namespace NUdf {

class TYqlMemoryPool : public arrow::MemoryPool {
    arrow::Status Allocate(int64_t size, uint8_t** out) final {
        Y_ENSURE(size >= 0 && out);
        *out = (uint8_t*)UdfArrowAllocate(size);
        UpdateAllocatedBytes(size);
        return arrow::Status::OK();
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) final {
        Y_ENSURE(old_size >= 0 && new_size >= 0 && ptr);
        *ptr = (uint8_t*)UdfArrowReallocate(*ptr, old_size, new_size);
        UpdateAllocatedBytes(new_size - old_size);
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size) final {
        Y_ENSURE(size >= 0);
        UdfArrowFree(buffer, size);
        UpdateAllocatedBytes(-size);
    }

    int64_t max_memory() const final {
        return max_memory_.load();
    }

    int64_t bytes_allocated() const final {
        return bytes_allocated_.load();
    }

    inline void UpdateAllocatedBytes(int64_t diff) {
        // inspired by arrow/memory_pool.h impl.
        int64_t allocated = bytes_allocated_.fetch_add(diff) + diff;
        if (diff > 0 && allocated > max_memory_) {
            max_memory_ = allocated;
        }
    }

    virtual std::string backend_name() const final {
        return "yql";
    }

private:
    std::atomic<int64_t> bytes_allocated_{0};
    std::atomic<int64_t> max_memory_{0};
};

arrow::MemoryPool* GetYqlMemoryPool() {
    return Singleton<TYqlMemoryPool>();
}

}
}

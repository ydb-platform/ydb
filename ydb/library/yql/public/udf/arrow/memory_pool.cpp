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
        return arrow::Status::OK();
    }
    
    arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) final {
        Y_ENSURE(old_size >= 0 && new_size >= 0 && ptr);
        *ptr = (uint8_t*)UdfArrowReallocate(*ptr, old_size, new_size);
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size) final {
        Y_ENSURE(size >= 0);
        UdfArrowFree(buffer, size);
    }

    virtual int64_t bytes_allocated() const final {
        return 0;
    }

    virtual std::string backend_name() const final {
        return "yql";
    }
};

arrow::MemoryPool* GetYqlMemoryPool() {
    return Singleton<TYqlMemoryPool>();
}

}
}

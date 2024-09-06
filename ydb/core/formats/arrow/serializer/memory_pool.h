#pragma once

#ifndef USE_CURRENT_UDF_ABI_VERSION
#   define USE_CURRENT_UDF_ABI_VERSION
#endif

#include <ydb/library/yql/public/udf/udf_version.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <arrow/memory_pool.h>

#include <util/generic/singleton.h>
#include <util/generic/yexception.h>

namespace NKikimr::NArrow {

class TMemoryPool : public arrow::MemoryPool {
    arrow::Status Allocate(int64_t size, uint8_t** out) final {
        Y_ENSURE(size >= 0 && out);
        *out = (uint8_t*)NMiniKQL::MKQLArrowAllocate(size);
        return arrow::Status::OK();
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) final {
        Y_ENSURE(old_size >= 0 && new_size >= 0 && ptr);
        *ptr = (uint8_t*)NMiniKQL::MKQLArrowReallocate(*ptr, old_size, new_size);
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size) final {
        Y_ENSURE(size >= 0);
        NMiniKQL::MKQLArrowFree(buffer, size);
    }

    int64_t bytes_allocated() const final {
        return 0;
    }

    std::string backend_name() const final {
        return "yql";
    }
};

static inline arrow::MemoryPool* GetMemoryPool() {
    return Singleton<TMemoryPool>();
}

} // namespace NKikimr::NArrow

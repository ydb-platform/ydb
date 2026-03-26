#pragma once
#include <yql/essentials/public/udf/udf_version.h>

#include <arrow/memory_pool.h>

namespace NYql::NUdf {

constexpr size_t ArrowMemoryAlignment = 64;
static_assert((ArrowMemoryAlignment & (ArrowMemoryAlignment - 1)) == 0, "ArrowMemoryAlignment should be power of 2");

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 37)
arrow20::MemoryPool* GetYqlMemoryPool();
#else
inline arrow20::MemoryPool* GetYqlMemoryPool() {
    return arrow20::default_memory_pool();
}
#endif

} // namespace NYql::NUdf

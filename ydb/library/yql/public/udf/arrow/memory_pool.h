#pragma once
#include <ydb/library/yql/public/udf/udf_version.h>

#include <arrow/memory_pool.h>

namespace NYql {
namespace NUdf {

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 37)
arrow::MemoryPool* GetYqlMemoryPool();
#else
inline arrow::MemoryPool* GetYqlMemoryPool() {
    return arrow::default_memory_pool();
}
#endif

}
}

#pragma once

#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/memory_pool.h>

namespace NKikimr::NMiniKQL {

std::unique_ptr<arrow::MemoryPool> MakeArrowMemoryPool(TAllocState& allocState);

}

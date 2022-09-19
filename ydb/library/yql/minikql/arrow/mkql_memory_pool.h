#pragma once

#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <arrow/memory_pool.h>

namespace NKikimr::NMiniKQL {

std::unique_ptr<arrow::MemoryPool> MakeArrowMemoryPool(TAllocState& allocState);

}

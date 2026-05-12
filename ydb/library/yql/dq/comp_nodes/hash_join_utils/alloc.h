#pragma once
#include <optional>
#include <yql/essentials/minikql/mkql_alloc.h>

namespace NKikimr::NMiniKQL {

int MemoryUsagePercent(int totalBytes);

int FreeMemory();

bool AllocateWithSizeMayThrow(i64 size);

std::optional<int> GetMemoryUsageIfReachedLimit();

bool MemoryPercentIsFree(int freePercent);
} // namespace NKikimr::NMiniKQL

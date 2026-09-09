#pragma once

#include <ydb/core/erasure/erasure.h>

// Benchmark-only declarations of the production functions. No codec arithmetic
// is implemented here, and these declarations are not part of erasure.h.
namespace NKikimr {
size_t ErasureSplitBlock42(std::span<TRope> parts, size_t offset, size_t size);
void ErasureRestoreBlock42ForBenchmark(std::span<TRope> parts, ui32 missingMask);
}

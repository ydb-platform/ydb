#pragma once

#include <util/system/types.h>

namespace NYql::NDq {

// Operator-facing view of the compute actor memory quota (RFC dq_memory_quota_20, section 3).
// Implemented by TDqMemoryQuota (ydb/library/yql/dq/actors/compute/dq_compute_memory_quota.h).
// Not thread safe: only the thread that runs the computation graph (the one that bound it) may use it.
class IDqOperatorMemoryQuota {
public:
    virtual ~IDqOperatorMemoryQuota() = default;

    // Raise the MKQL allocator limit by at least `bytes` (the owner rounds up to its allocation step).
    // isOptional == true : never throws; the quota manager may refuse in advance even if free quota exists,
    //                      the caller must be able to continue without the memory (spill, drain, shrink).
    // isOptional == false: same semantics as the implicit allocator callback.
    // Returns true when the limit was raised.
    virtual bool RequestExtraMemory(ui64 bytes, bool isOptional) = 0;

    // > 0: bytes that may still be requested; 0: do not request optional memory;
    // < 0: over target, please give |value| bytes back (free memory, then call TryShrinkMemory()).
    virtual i64 GetMemoryAvailability() const = 0;

    // Release free allocator pages and return the unused part of the limit to the quota manager.
    virtual void TryShrinkMemory() = 0;
};

// Quota bound to the current thread by the owner of the computation graph (compute actor or task runner
// actor) for the duration of graph execution. nullptr when unbound: literal executer, unit tests, YQL DQ
// workers, graph construction and teardown. Operators must then fall back to the allocator heuristics
// (TlsAllocState->IsMemoryYellowZoneEnabled() / GetMaximumLimitValueReached()).
IDqOperatorMemoryQuota* GetDqOperatorMemoryQuota();

// RAII binding, nestable: the destructor restores the previously bound quota. `quota` may be nullptr.
class TDqOperatorMemoryQuotaScope {
public:
    explicit TDqOperatorMemoryQuotaScope(IDqOperatorMemoryQuota* quota);
    ~TDqOperatorMemoryQuotaScope();

    TDqOperatorMemoryQuotaScope(const TDqOperatorMemoryQuotaScope&) = delete;
    TDqOperatorMemoryQuotaScope& operator=(const TDqOperatorMemoryQuotaScope&) = delete;

private:
    IDqOperatorMemoryQuota* const Previous;
};

} // namespace NYql::NDq

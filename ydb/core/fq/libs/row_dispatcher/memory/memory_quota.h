#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <yql/essentials/minikql/mkql_alloc.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq::NRowDispatcher {

// Retain reservations until destruction; Resize tracks usage without releasing quota.
class TMemoryQuota : private TNonCopyable {
public:
    explicit TMemoryQuota(NYql::NDq::IMemoryQuotaManager::TPtr manager = {}, TString memoryName = "BufferMemory", NMonitoring::TDynamicCounterPtr counters = {});
    ~TMemoryQuota();

    void Add(ui64 delta);
    void Resize(ui64 size);
    void Reserve(ui64 size);
    ui64 GetSize() const;

private:
    const NYql::NDq::IMemoryQuotaManager::TPtr Manager;
    const TString MemoryName;
    const NMonitoring::TDynamicCounters::TCounterPtr ReservedBytes;
    ui64 CurrentSize = 0;
    ui64 AllocatedSize = 0;
};

TString GetMemoryLimitExceededMessage(const NKikimr::TMemoryLimitExceededException& error, TStringBuf context = {});
void LimitAllocator(NKikimr::NMiniKQL::TScopedAlloc& alloc, const NYql::NDq::IMemoryQuotaManager::TPtr& manager, TString memoryName = "MkqlAlloc", NMonitoring::TDynamicCounterPtr counters = {});

} // namespace NFq::NRowDispatcher

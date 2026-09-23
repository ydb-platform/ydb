#pragma once

#include <ydb/core/fq/libs/row_dispatcher/memory/memory_quota.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

#include <memory>

namespace NFq::NRowDispatcher {

class TMemoryLimitedDataPacker {
public:
    TMemoryLimitedDataPacker(NYql::NDq::IMemoryQuotaManager::TPtr manager, ui64 itemSizeOverhead, NMonitoring::TDynamicCounterPtr counters = {});

    size_t PackedSizeEstimate() const;

    bool IsEmpty() const;

    void SetPackerType(const NKikimr::NMiniKQL::TType* type);

    void AddWideItem(const NYql::NUdf::TUnboxedValuePod* values, ui32 count);

    std::pair<NYql::TChunkedBuffer, ui64> Finish();

private:
    ui64 EstimateMemoryUsage(size_t packedSize) const;

    const ui64 ItemSizeOverhead = 0;
    TMemoryQuota PackingMemory;
    std::unique_ptr<NKikimr::NMiniKQL::TValuePackerTransport<true>> Packer;
    ui64 ItemsCount = 0;
};

} // namespace NFq::NRowDispatcher

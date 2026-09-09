#pragma once

#include <ydb/core/fq/libs/row_dispatcher/memory/memory_quota.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

namespace NFq::NRowDispatcher {

class TMemoryLimitedDataPacker {
public:
    TMemoryLimitedDataPacker(const NKikimr::NMiniKQL::TType* type, NYql::NDq::IMemoryQuotaManager::TPtr manager);

    void AddWideItem(const NYql::NUdf::TUnboxedValuePod* values, ui32 count);
    size_t PackedSizeEstimate() const;
    bool IsEmpty() const;
    NYql::TChunkedBuffer Finish();

private:
    const NYql::NDq::IMemoryQuotaManager::TPtr Manager;
    std::shared_ptr<TMemoryQuota> Memory;
    NKikimr::NMiniKQL::TValuePackerTransport<true> Packer;
};

} // namespace NFq::NRowDispatcher

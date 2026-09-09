#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/utils/chunked_buffer.h>

namespace NFq::NRowDispatcher {

class TMemoryQuota : private TNonCopyable {
public:
    explicit TMemoryQuota(NYql::NDq::IMemoryQuotaManager::TPtr manager = {});
    ~TMemoryQuota();

    void Resize(ui64 size);
    void Reserve(ui64 size);
    ui64 GetSize() const;

private:
    const NYql::NDq::IMemoryQuotaManager::TPtr Manager;
    ui64 Size = 0;
};

void LimitAllocator(NKikimr::NMiniKQL::TScopedAlloc& alloc, const NYql::NDq::IMemoryQuotaManager::TPtr& manager);
NYql::TChunkedBuffer HoldMemoryQuota(NYql::TChunkedBuffer buffer, std::shared_ptr<TMemoryQuota> quota);

} // namespace NFq::NRowDispatcher

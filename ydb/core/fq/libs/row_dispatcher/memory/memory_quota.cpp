#include "memory_quota.h"

#include <util/generic/size_literals.h>
#include <util/system/align.h>

namespace NFq::NRowDispatcher {

TMemoryQuota::TMemoryQuota(NYql::NDq::IMemoryQuotaManager::TPtr manager)
    : Manager(std::move(manager))
{}

TMemoryQuota::~TMemoryQuota() {
    Resize(0);
}

void TMemoryQuota::Resize(ui64 size) {
    if (Manager) {
        if (size > Size && !Manager->AllocateQuota(size - Size)) {
            throw NKikimr::TMemoryLimitExceededException();
        }
        if (size < Size) {
            Manager->FreeQuota(Size - size);
        }
    }
    Size = size;
}

void TMemoryQuota::Reserve(ui64 size) {
    if (size > Size) {
        Resize(size);
    }
}

ui64 TMemoryQuota::GetSize() const {
    return Size;
}

void LimitAllocator(NKikimr::NMiniKQL::TScopedAlloc& alloc, const NYql::NDq::IMemoryQuotaManager::TPtr& manager) {
    if (!manager) {
        return;
    }

    auto quota = std::make_shared<TMemoryQuota>(manager);
    quota->Resize(std::max<ui64>(alloc.GetAllocated(), 1));
    alloc.SetLimit(quota->GetSize());
    alloc.Ref().SetIncreaseMemoryLimitCallback([quota, &alloc](ui64, ui64 required) {
        constexpr ui64 step = 1_MB;
        quota->Resize(AlignUp(required, step));
        alloc.SetLimit(quota->GetSize());
    });
}

NYql::TChunkedBuffer HoldMemoryQuota(NYql::TChunkedBuffer buffer, std::shared_ptr<TMemoryQuota> quota) {
    struct TOwner {
        std::shared_ptr<TMemoryQuota> Quota;
        std::shared_ptr<const void> Data;
    };
    NYql::TChunkedBuffer result;
    while (!buffer.Empty()) {
        const auto& chunk = buffer.Front();
        result.Append(chunk.Buf, std::make_shared<TOwner>(TOwner{quota, chunk.Owner}));
        buffer.Erase(chunk.Buf.size());
    }
    return result;
}

} // namespace NFq::NRowDispatcher

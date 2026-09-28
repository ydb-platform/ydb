#include "memory_quota.h"

#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/system/align.h>

namespace NFq::NRowDispatcher {

namespace {

constexpr ui64 MIN_MEMORY_DELTA = 1_MB;

class TMemoryQuotaExceededException : public NKikimr::TMemoryLimitExceededException {
public:
    explicit TMemoryQuotaExceededException(TString details)
        : Details(std::move(details))
    {}

    const TString Details;
};

} // anonymous namespace

TMemoryQuota::TMemoryQuota(NYql::NDq::IMemoryQuotaManager::TPtr manager, TString memoryName, NMonitoring::TDynamicCounterPtr counters)
    : Manager(std::move(manager))
    , MemoryName(std::move(memoryName))
    , ReservedBytes(counters ? counters->GetSubgroup("component", "MemoryQuota")->GetCounter(MemoryName) : nullptr)
{}

TMemoryQuota::~TMemoryQuota() {
    if (Manager && AllocatedSize) {
        Manager->FreeQuota(AllocatedSize);
    }
    if (ReservedBytes) {
        ReservedBytes->Sub(AllocatedSize);
    }
}

void TMemoryQuota::Resize(ui64 size) {
    if (size > AllocatedSize) {
        const ui64 delta = AlignUp(size - AllocatedSize, MIN_MEMORY_DELTA);
        if (Manager && !Manager->AllocateQuota(delta, /* isOptional */ false)) {
            throw TMemoryQuotaExceededException(TStringBuilder()
                << "failed to reserve " << delta << " bytes for " << MemoryName
                << " (already reserved: " << AllocatedSize << " bytes, actually used bytes " << CurrentSize << ")");
        }
        AllocatedSize += delta;
        if (ReservedBytes) {
            ReservedBytes->Add(delta);
        }
    }

    CurrentSize = size;
}

void TMemoryQuota::Add(ui64 delta) {
    Resize(CurrentSize + delta);
}

void TMemoryQuota::Reserve(ui64 size) {
    if (size > CurrentSize) {
        Resize(size);
    }
}

ui64 TMemoryQuota::GetSize() const {
    return CurrentSize;
}

TString GetMemoryLimitExceededMessage(const NKikimr::TMemoryLimitExceededException& error, TStringBuf context) {
    TStringBuilder message;
    message << "Row dispatcher memory limit exceeded";
    if (context) {
        message << " " << context;
    }
    if (const auto* quotaError = dynamic_cast<const TMemoryQuotaExceededException*>(&error)) {
        message << ": " << quotaError->Details;
    }
    return message;
}

void LimitAllocator(NKikimr::NMiniKQL::TScopedAlloc& alloc, const NYql::NDq::IMemoryQuotaManager::TPtr& manager, TString memoryName, NMonitoring::TDynamicCounterPtr counters) {
    if (!manager && !counters) {
        return;
    }

    // Without a manager, account the same reservations but allow unlimited growth.
    auto quota = std::make_shared<TMemoryQuota>(manager, std::move(memoryName), std::move(counters));
    quota->Resize(AlignUp(std::max<ui64>(alloc.GetAllocated(), 1), MIN_MEMORY_DELTA));
    alloc.SetLimit(quota->GetSize());
    alloc.Ref().SetIncreaseMemoryLimitCallback([quota, &alloc](ui64, ui64 required) {
        quota->Resize(AlignUp(required, MIN_MEMORY_DELTA));
        alloc.SetLimit(quota->GetSize());
    });
}

} // namespace NFq::NRowDispatcher

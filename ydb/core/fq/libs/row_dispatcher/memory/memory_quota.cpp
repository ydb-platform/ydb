#include "memory_quota.h"

#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/system/align.h>

namespace NFq::NRowDispatcher {

namespace {

class TMemoryQuotaExceededException : public NKikimr::TMemoryLimitExceededException {
public:
    explicit TMemoryQuotaExceededException(TString details)
        : Details(std::move(details))
    {}

    const TString Details;
};

} // anonymous namespace

TMemoryQuota::TMemoryQuota(NYql::NDq::IMemoryQuotaManager::TPtr manager, TString memoryName)
    : Manager(std::move(manager))
    , MemoryName(std::move(memoryName))
{}

TMemoryQuota::~TMemoryQuota() {
    Resize(0);
}

void TMemoryQuota::Resize(ui64 size) {
    if (Manager) {
        if (size > Size && !Manager->AllocateQuota(size - Size)) {
            throw TMemoryQuotaExceededException(TStringBuilder()
                << "failed to reserve " << size - Size << " bytes for " << MemoryName
                << " (already reserved: " << Size << " bytes)");
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

void LimitAllocator(NKikimr::NMiniKQL::TScopedAlloc& alloc, const NYql::NDq::IMemoryQuotaManager::TPtr& manager, TString memoryName) {
    if (!manager) {
        return;
    }

    auto quota = std::make_shared<TMemoryQuota>(manager, std::move(memoryName));
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

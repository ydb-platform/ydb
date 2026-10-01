#include "dq_arrow_memory_pool.h"

#include <util/generic/singleton.h>
#include <util/system/tls.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <cstring>
#include <new>

namespace NYql::NDq {

namespace {

// arrow buffers are 64 byte aligned, the header keeps the data aligned
constexpr int64_t HeaderSize = 64;

struct THeader {
    // nullptr: not charged
    IMemoryQuotaManager::TPtr Quota;
    int64_t Size;
};

static_assert(sizeof(THeader) <= HeaderSize);

alignas(HeaderSize) uint8_t ZeroSizeArea[1];

Y_POD_STATIC_THREAD(const IMemoryQuotaManager::TPtr*) TlsArrowMemoryQuota;

std::atomic<bool> ArrowMemoryQuotaEnabled = false;

THeader* GetHeader(uint8_t* buffer) {
    return reinterpret_cast<THeader*>(buffer - HeaderSize);
}

arrow::Status QuotaExceeded(const IMemoryQuotaManager& quota, int64_t size) {
    return arrow::Status::OutOfMemory("arrow memory quota exceeded, allocating ", size, " bytes. ",
        quota.MemoryConsumptionDetails());
}

// the charge is taken already, returned on failure
arrow::Status AllocateWithHeader(IMemoryQuotaManager::TPtr quota, int64_t size, uint8_t** out) {
    uint8_t* raw = nullptr;
    if (auto status = arrow::system_memory_pool()->Allocate(size + HeaderSize, &raw); !status.ok()) {
        if (quota) {
            quota->FreeQuota(size);
        }
        return status;
    }

    new (raw) THeader{std::move(quota), size};
    *out = raw + HeaderSize;
    return arrow::Status::OK();
}

void FreeWithHeader(uint8_t* buffer) {
    auto* header = GetHeader(buffer);
    const int64_t size = header->Size;
    header->~THeader();
    arrow::system_memory_pool()->Free(reinterpret_cast<uint8_t*>(header), size + HeaderSize);
}

} // namespace

arrow::Status TDqArrowMemoryPool::Allocate(int64_t size, uint8_t** out) {
    if (size < 0) {
        return arrow::Status::Invalid("negative allocation size ", size);
    }
    if (size == 0) {
        *out = ZeroSizeArea;
        return arrow::Status::OK();
    }

    IMemoryQuotaManager::TPtr quota;
    if (const auto* bound = TlsArrowMemoryQuota; bound && *bound) {
        quota = *bound;
        if (!quota->AllocateQuota(size, /* isOptional */ false)) {
            return QuotaExceeded(*quota, size);
        }
    }

    ARROW_RETURN_NOT_OK(AllocateWithHeader(std::move(quota), size, out));
    UpdateAllocatedBytes(size);
    return arrow::Status::OK();
}

arrow::Status TDqArrowMemoryPool::Reallocate(int64_t oldSize, int64_t newSize, uint8_t** ptr) {
    if (newSize < 0) {
        return arrow::Status::Invalid("negative reallocation size ", newSize);
    }
    if (*ptr == ZeroSizeArea) {
        return Allocate(newSize, ptr);
    }
    if (newSize == 0) {
        Free(*ptr, oldSize);
        *ptr = ZeroSizeArea;
        return arrow::Status::OK();
    }

    auto* header = GetHeader(*ptr);
    Y_DEBUG_ABORT_UNLESS(header->Size == oldSize, "reallocating %" PRIi64 " bytes of a %" PRIi64 " byte buffer",
        oldSize, header->Size);
    oldSize = header->Size;

    // the buffer stays charged to its own manager, whatever is bound now
    IMemoryQuotaManager::TPtr quota = header->Quota;
    if (quota && newSize > oldSize && !quota->AllocateQuota(newSize - oldSize, /* isOptional */ false)) {
        return QuotaExceeded(*quota, newSize - oldSize);
    }

    uint8_t* raw = nullptr;
    if (auto status = arrow::system_memory_pool()->Allocate(newSize + HeaderSize, &raw); !status.ok()) {
        if (quota && newSize > oldSize) {
            quota->FreeQuota(newSize - oldSize);
        }
        return status;
    }

    new (raw) THeader{quota, newSize};
    std::memcpy(raw + HeaderSize, *ptr, std::min(oldSize, newSize));
    FreeWithHeader(*ptr);
    *ptr = raw + HeaderSize;

    if (quota && newSize < oldSize) {
        quota->FreeQuota(oldSize - newSize);
    }
    UpdateAllocatedBytes(newSize - oldSize);
    return arrow::Status::OK();
}

void TDqArrowMemoryPool::Free(uint8_t* buffer, int64_t size) {
    if (buffer == ZeroSizeArea) {
        return;
    }

    auto* header = GetHeader(buffer);
    Y_DEBUG_ABORT_UNLESS(header->Size == size, "freeing %" PRIi64 " bytes of a %" PRIi64 " byte buffer",
        size, header->Size);
    size = header->Size;
    if (header->Quota) {
        header->Quota->FreeQuota(size);
    }
    FreeWithHeader(buffer);
    UpdateAllocatedBytes(-size);
}

void TDqArrowMemoryPool::UpdateAllocatedBytes(int64_t diff) {
    const int64_t allocated = BytesAllocated.fetch_add(diff) + diff;
    int64_t peak = MaxMemory.load();
    while (peak < allocated && !MaxMemory.compare_exchange_weak(peak, allocated)) {
    }
}

TDqArrowMemoryPool* GetDqArrowMemoryPool() {
    return Singleton<TDqArrowMemoryPool>();
}

IMemoryQuotaManager* GetArrowMemoryQuota() {
    const auto* bound = TlsArrowMemoryQuota;
    return bound ? bound->get() : nullptr;
}

TArrowMemoryQuotaScope::TArrowMemoryQuotaScope(IMemoryQuotaManager::TPtr quota)
    : Quota(std::move(quota))
    , Prev(TlsArrowMemoryQuota)
{
    TlsArrowMemoryQuota = &Quota;
}

TArrowMemoryQuotaScope::~TArrowMemoryQuotaScope() {
    Y_DEBUG_ABORT_UNLESS(TlsArrowMemoryQuota == &Quota, "arrow memory quota scopes must be closed in reverse order");
    TlsArrowMemoryQuota = Prev;
}

void SetArrowMemoryQuotaEnabled(bool enabled) {
    ArrowMemoryQuotaEnabled.store(enabled);
}

bool IsArrowMemoryQuotaEnabled() {
    return ArrowMemoryQuotaEnabled.load();
}

} // namespace NYql::NDq

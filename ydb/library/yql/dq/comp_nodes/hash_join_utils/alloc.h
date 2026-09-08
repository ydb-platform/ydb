#pragma once

#include <yql/essentials/minikql/mkql_alloc.h>

#include <util/system/types.h>

#include <utility>

namespace NKikimr::NMiniKQL {

// Accounts memory that bypasses the MKQL allocator (the serialized copies handed to the spiller, the chunked
// buffers produced by the spilling actor) in the current TAllocState with OffloadAlloc/OffloadFree
// (RFC dq_memory_quota_20, 3.1). Enabled only while the operator memory quota is bound, otherwise it just
// carries the size. Must be created and destroyed on the thread that runs the graph under its allocator:
// OffloadAlloc runs the mandatory limit callback and may throw TMemoryLimitExceededException.
class TOffloadedMemoryGuard {
public:
    TOffloadedMemoryGuard() = default;

    TOffloadedMemoryGuard(ui64 bytes, bool enabled)
        : Bytes_(bytes)
        , Accounted_(enabled && bytes > 0)
    {
        if (Accounted_) {
            TlsAllocState->OffloadAlloc(Bytes_);
        }
    }

    TOffloadedMemoryGuard(TOffloadedMemoryGuard&& other) noexcept
        : Bytes_(std::exchange(other.Bytes_, 0))
        , Accounted_(std::exchange(other.Accounted_, false))
    {
    }

    TOffloadedMemoryGuard& operator=(TOffloadedMemoryGuard&& other) noexcept {
        if (this != &other) {
            Release();
            Bytes_ = std::exchange(other.Bytes_, 0);
            Accounted_ = std::exchange(other.Accounted_, false);
        }
        return *this;
    }

    TOffloadedMemoryGuard(const TOffloadedMemoryGuard&) = delete;
    TOffloadedMemoryGuard& operator=(const TOffloadedMemoryGuard&) = delete;

    ~TOffloadedMemoryGuard() {
        Release();
    }

    void Release() noexcept {
        if (Accounted_) {
            Accounted_ = false;
            if (TlsAllocState) {
                TlsAllocState->OffloadFree(Bytes_);
            }
        }
    }

    ui64 Bytes() const {
        return Bytes_;
    }

private:
    ui64 Bytes_ = 0;
    bool Accounted_ = false;
};

} // namespace NKikimr::NMiniKQL

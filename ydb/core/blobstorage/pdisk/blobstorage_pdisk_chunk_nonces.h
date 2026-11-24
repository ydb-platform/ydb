#pragma once

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <atomic>
#include <optional>
#include <vector>

namespace NKikimr::NPDisk {

// One table per PDisk. The PDisk publishes the nonce before replying to an allocation.
// Readers learn the chunk index through actor messages. Zero denotes an unavailable nonce.
class TChunkNonces {
public:
    explicit TChunkNonces(ui32 chunkCount)
        : Nonces(chunkCount)
    {
        static_assert(std::atomic<ui64>::is_always_lock_free);
        for (std::atomic<ui64>& nonce : Nonces) {
            nonce.store(0, std::memory_order_relaxed);
        }
    }

    void Set(ui32 chunkIdx, ui64 nonce) {
        Y_ABORT_UNLESS(chunkIdx < Nonces.size());
        Nonces[chunkIdx].store(nonce, std::memory_order_relaxed);
    }

    std::optional<ui64> Get(ui32 chunkIdx) const {
        if (chunkIdx < Nonces.size()) {
            if (const ui64 nonce = Nonces[chunkIdx].load(std::memory_order_relaxed)) {
                return nonce;
            }
        }
        return std::nullopt;
    }

private:
    std::vector<std::atomic<ui64>> Nonces;
};

} // namespace NKikimr::NPDisk

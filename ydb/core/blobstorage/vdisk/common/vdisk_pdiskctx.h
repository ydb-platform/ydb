#pragma once
#include "defs.h"

#include "vdisk_config.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_params.h>

#include <atomic>
#include <memory>
#include <optional>
#include <vector>


namespace NKikimr {

    namespace NPDisk {
        class TFormatContext;
    }

    class TPDiskCtx;
    using TPDiskCtxPtr = std::shared_ptr<TPDiskCtx>;

    ////////////////////////////////////////////////////////////////////////////
    // Nonces of the chunks this VDisk owns, for sealing sectors without the PDisk. Filled from
    // TEvYardInitResult and from every TEvChunkReserveResult; a chunk index gets a new nonce each time it
    // is reserved again, so a reservation must always be recorded, and a chunk allocated by a write (sync log)
    // must be forgotten. A missing entry only means the ordinary write path is used; a wrong one makes PDisk
    // refuse the write with ERROR, which stops the VDisk.
    ////////////////////////////////////////////////////////////////////////////
    class TChunkNonces {
    public:
        explicit TChunkNonces(ui32 chunkCount)
            : Nonces(chunkCount)
        {
            static_assert(std::atomic<ui64>::is_always_lock_free);
            for (auto& nonce : Nonces) {
                nonce.store(0, std::memory_order_relaxed);
            }
        }

        void Record(const TVector<ui32>& chunks, const TVector<ui64>& nonces) {
            if (chunks.size() != nonces.size()) {
                return; // plain data chunks: no nonces
            }
            for (size_t i = 0; i < chunks.size(); ++i) {
                if (chunks[i] < Nonces.size()) {
                    Nonces[chunks[i]].store(nonces[i], std::memory_order_relaxed);
                }
            }
        }

        void Forget(ui32 chunkIdx) {
            if (chunkIdx < Nonces.size()) {
                Nonces[chunkIdx].store(0, std::memory_order_relaxed);
            }
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
        // Fixed at YardInit. A zero nonce or a chunk added after init uses ordinary PDisk writes.
        // Only the nonce is shared; publishing it does not publish any other actor state.
        std::vector<std::atomic<ui64>> Nonces;
    };

    ////////////////////////////////////////////////////////////////////////////
    // This context contains info about PDisk that VDisk needs to know (not more)
    ////////////////////////////////////////////////////////////////////////////
    class TPDiskCtx {
    public:
        // PDisk related constants
        const TIntrusivePtr<TPDiskParams> Dsk;
        // PDisk Actor id
        const TActorId PDiskId;
        // PDisk id string
        const TString PDiskIdString;
        // Set from TEvYardInitResult; null for plain data chunks.
        std::shared_ptr<const NPDisk::TFormatContext> FormatContext;
        const std::shared_ptr<TChunkNonces> ChunkNonces;
        // VDiskControls.EnableVDiskSectorPreparation, shared with the ICB: read per write.
        TControlWrapper EnableSectorPreparation;

        TPDiskCtx(TIntrusivePtr<TPDiskParams> dsk, TActorId pDiskId, TString pdiskIdString, ui32 chunkCount = 0)
            : Dsk(std::move(dsk))
            , PDiskId(pDiskId)
            , PDiskIdString(pdiskIdString)
            , ChunkNonces(std::make_shared<TChunkNonces>(chunkCount))
        {}

        static TPDiskCtxPtr Create(
                const TIntrusivePtr<TPDiskParams> &pDiskParams,
                const TIntrusivePtr<TVDiskConfig> &cfg, ui32 chunkCount = 0) {
            auto pdiskCtx = std::make_shared<TPDiskCtx>(pDiskParams, cfg->BaseInfo.PDiskActorID,
                TStringBuilder() << TActivationContext::ActorSystem()->NodeId << ":" << cfg->BaseInfo.PDiskId, chunkCount);
            Y_ABORT_UNLESS(cfg->MaxLogoBlobDataSize < pdiskCtx->Dsk->ChunkSize + 1024u,
                    "Chunk size is too small, check your VDisk settings; "
                    "MaxLogoBlobDataSize=%" PRIu32 " chunkSize=%" PRIu64,
                    cfg->MaxLogoBlobDataSize, pdiskCtx->Dsk->ChunkSize);
            pdiskCtx->EnableSectorPreparation = cfg->EnableVDiskSectorPreparation;
            return pdiskCtx;
        }
    };

} // NKikimr

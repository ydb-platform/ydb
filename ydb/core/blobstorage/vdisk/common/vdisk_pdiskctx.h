#pragma once
#include "defs.h"

#include "vdisk_config.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_params.h>

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

#include <optional>


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
        void Record(const TVector<ui32>& chunks, const TVector<ui64>& nonces) {
            if (chunks.size() != nonces.size()) {
                return; // plain data chunks: no nonces
            }
            auto guard = Guard(Lock);
            for (size_t i = 0; i < chunks.size(); ++i) {
                Nonces[chunks[i]] = nonces[i];
            }
        }

        void Forget(ui32 chunkIdx) {
            auto guard = Guard(Lock);
            Nonces.erase(chunkIdx);
        }

        std::optional<ui64> Get(ui32 chunkIdx) const {
            auto guard = Guard(Lock);
            if (const auto it = Nonces.find(chunkIdx); it != Nonces.end()) {
                return it->second;
            }
            return std::nullopt;
        }

    private:
        TAdaptiveLock Lock;
        THashMap<ui32, ui64> Nonces;
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
        const std::shared_ptr<TChunkNonces> ChunkNonces = std::make_shared<TChunkNonces>();
        // VDiskControls.EnableVDiskSectorPreparation, shared with the ICB: read per write.
        TControlWrapper EnableSectorPreparation;

        TPDiskCtx(TIntrusivePtr<TPDiskParams> dsk, TActorId pDiskId, TString pdiskIdString)
            : Dsk(std::move(dsk))
            , PDiskId(pDiskId)
            , PDiskIdString(pdiskIdString)
        {}

        static TPDiskCtxPtr Create(
                const TIntrusivePtr<TPDiskParams> &pDiskParams,
                const TIntrusivePtr<TVDiskConfig> &cfg) {
            auto pdiskCtx = std::make_shared<TPDiskCtx>(pDiskParams, cfg->BaseInfo.PDiskActorID,
                TStringBuilder() << TActivationContext::ActorSystem()->NodeId << ":" << cfg->BaseInfo.PDiskId);
            Y_ABORT_UNLESS(cfg->MaxLogoBlobDataSize < pdiskCtx->Dsk->ChunkSize + 1024u,
                    "Chunk size is too small, check your VDisk settings; "
                    "MaxLogoBlobDataSize=%" PRIu32 " chunkSize=%" PRIu64,
                    cfg->MaxLogoBlobDataSize, pdiskCtx->Dsk->ChunkSize);
            pdiskCtx->EnableSectorPreparation = cfg->EnableVDiskSectorPreparation;
            return pdiskCtx;
        }
    };

} // NKikimr

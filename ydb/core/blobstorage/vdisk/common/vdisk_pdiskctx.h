#pragma once
#include "defs.h"

#include "vdisk_config.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_chunk_nonces.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_params.h>

#include <memory>


namespace NKikimr {

    namespace NPDisk {
        class TFormatContext;
    }

    class TPDiskCtx;
    using TPDiskCtxPtr = std::shared_ptr<TPDiskCtx>;

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
        const std::shared_ptr<const NPDisk::TChunkNonces> ChunkNonces;
        // VDiskControls.EnableVDiskSectorPreparation, shared with the ICB: read per write.
        TControlWrapper EnableSectorPreparation;

        TPDiskCtx(TIntrusivePtr<TPDiskParams> dsk, TActorId pDiskId, TString pdiskIdString,
                std::shared_ptr<const NPDisk::TChunkNonces> chunkNonces = {})
            : Dsk(std::move(dsk))
            , PDiskId(pDiskId)
            , PDiskIdString(pdiskIdString)
            , ChunkNonces(std::move(chunkNonces))
        {}

        static TPDiskCtxPtr Create(
                const TIntrusivePtr<TPDiskParams> &pDiskParams,
                const TIntrusivePtr<TVDiskConfig> &cfg, std::shared_ptr<const NPDisk::TChunkNonces> chunkNonces = {}) {
            auto pdiskCtx = std::make_shared<TPDiskCtx>(pDiskParams, cfg->BaseInfo.PDiskActorID,
                TStringBuilder() << TActivationContext::ActorSystem()->NodeId << ":" << cfg->BaseInfo.PDiskId, std::move(chunkNonces));
            Y_ABORT_UNLESS(cfg->MaxLogoBlobDataSize < pdiskCtx->Dsk->ChunkSize + 1024u,
                    "Chunk size is too small, check your VDisk settings; "
                    "MaxLogoBlobDataSize=%" PRIu32 " chunkSize=%" PRIu64,
                    cfg->MaxLogoBlobDataSize, pdiskCtx->Dsk->ChunkSize);
            pdiskCtx->EnableSectorPreparation = cfg->EnableVDiskSectorPreparation;
            return pdiskCtx;
        }
    };

} // NKikimr

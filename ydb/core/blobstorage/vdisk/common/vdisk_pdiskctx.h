#pragma once
#include "defs.h"

#include "vdisk_config.h"
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr {

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

        TPDiskCtx(TIntrusivePtr<TPDiskParams> dsk, TActorId pDiskId, TString pdiskIdString)
            : Dsk(std::move(dsk))
            , PDiskId(pDiskId)
            , PDiskIdString(pdiskIdString)
        {}

        static TPDiskCtxPtr Create(
                const TIntrusivePtr<TPDiskParams> &pDiskParams,
                const TIntrusivePtr<TVDiskConfig> &cfg) {
            auto pdiskCtx = std::make_shared<TPDiskCtx>(pDiskParams, cfg->BaseInfo.PDiskActorID,
                TStringBuilder() << TlsActivationContext->ExecutorThread.ActorSystem->NodeId << ":" << cfg->BaseInfo.PDiskId);
            Y_ABORT_UNLESS(cfg->MaxLogoBlobDataSize < pdiskCtx->Dsk->ChunkSize + 1024u,
                    "Chunk size is too small, check your VDisk settings; "
                    "MaxLogoBlobDataSize=%" PRIu32 " chunkSize=%" PRIu64,
                    cfg->MaxLogoBlobDataSize, pdiskCtx->Dsk->ChunkSize);
            return pdiskCtx;
        }
    };

} // NKikimr

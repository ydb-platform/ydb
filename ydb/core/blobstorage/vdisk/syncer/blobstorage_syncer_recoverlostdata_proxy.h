#pragma once

#include "defs.h"
#include "blobstorage_syncer_defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>

namespace NKikimr {

    class TSyncerContext;

    namespace NSyncer {
        struct TSjCtx;
        struct TPeerSyncState;
    } // NSyncer


    ////////////////////////////////////////////////////////////////////////////
    // TEvSyncerFullSyncedWithPeer
    ////////////////////////////////////////////////////////////////////////////
    struct TEvSyncerFullSyncedWithPeer
        : public TEventLocal<TEvSyncerFullSyncedWithPeer,
                             TEvBlobStorage::EvSyncerFullSyncedWithPeer>
    {
        // Fully synced with this vdisk
        const TVDiskIdShort VDiskId;

        TEvSyncerFullSyncedWithPeer(const TVDiskIdShort &vdisk)
            : VDiskId(vdisk)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerRLDFullSyncProxyActor CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateProxyForFullSyncWithPeer(const TIntrusivePtr<TSyncerContext> &sc,
                                           const NSyncer::TPeerSyncState& peerSyncState,
                                           const TActorId &committerId,
                                           const TActorId &notifyId,
                                           const std::shared_ptr<NSyncer::TSjCtx> &jobCtx,
                                           const TVDiskID &targetVDiskId,
                                           const TActorId &targetActorId);

} // NKikimr

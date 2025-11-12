#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

namespace NKikimr {
    // Actor for legacy protocol
    // New instance of the actor is created to prepare every TEvVSyncFullResult message
    // Reads part of snapshot, starting from given key*
    // Passes away when TEvVSyncFullResult is sent 
    class TDb;
    IActor* CreateHullSyncFullActorLegacyProtocol(
            const TIntrusivePtr<TVDiskConfig> &config,
            const TIntrusivePtr<THullCtx> &hullCtx,
            const TActorId &parentId,
            THullDsSnap &&fullSnap,
            const TSyncState& syncState,
            const TVDiskID& selfVDiskId,
            const std::shared_ptr<NMonGroup::TVDiskIFaceGroup>& ifaceMonGroup,
            const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
            const TEvBlobStorage::TEvVSyncFull::TPtr& ev,
            const TKeyLogoBlob &keyLogoBlob,
            const TKeyBlock &keyBlock,
            const TKeyBarrier &keyBarrier,
            NKikimrBlobStorage::ESyncFullStage stage);

    // Actor for new protocol
    // Is created only once per FullSync session
    // Passes away when all data is sent
    // Doesn't require key* fields, because it knows snapshot iterator
    // Passes away on Poison or when all data is sent
    IActor* CreateHullSyncFullActorUnorderedDataProtocol(
        const TIntrusivePtr<TVDiskConfig> &config,
        const TIntrusivePtr<THullCtx> &hullCtx,
        const TActorId &parentId,
        const TActorId& syncLogActorId,
        THullDsSnap &&fullSnap,
        const TSyncState& syncState,
        const TVDiskID& selfVDiskId,
        const std::shared_ptr<NMonGroup::TVDiskIFaceGroup>& ifaceMonGroup,
        const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
        const TEvBlobStorage::TEvVSyncFull::TPtr& ev);

} // NKikimr

#pragma once

#include "defs.h"
#include "localrecovery_defs.h"
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimrBlobStorage {

    class TLocalRecoveryInfo;

} // NKikimrBlobStorage

namespace NKikimr {

    class TVDiskContext;
    struct TSyncerData;
    class THullDbRecovery;
    struct TVDiskConfig;
    class TPDiskCtx;
    struct THullCtx;
    class THugeBlobCtx;
    class TLsnMngr;

    namespace NSyncLog {
        struct TSyncLogRepaired;
    } // NSyncLog

    namespace NHuge {
        struct THullHugeKeeperPersState;
    }

    ////////////////////////////////////////////////////////////////////////////
    // TEvLocalRecoveryDone -- local recovery done message
    ////////////////////////////////////////////////////////////////////////////
    struct TEvBlobStorage::TEvLocalRecoveryDone
        : public TEventLocal<TEvBlobStorage::TEvLocalRecoveryDone, TEvBlobStorage::EvLocalRecoveryDone>
    {
        NKikimrProto::EReplyStatus Status;
        TIntrusivePtr<TLocalRecoveryInfo> RecovInfo;
        std::unique_ptr<NSyncLog::TSyncLogRepaired> RepairedSyncLog;
        std::shared_ptr<NHuge::THullHugeKeeperPersState> RepairedHuge;
        TIntrusivePtr<TSyncerData> SyncerData;
        std::shared_ptr<THullDbRecovery> Uncond;
        std::shared_ptr<TPDiskCtx> PDiskCtx;
        TIntrusivePtr<THullCtx> HullCtx;
        std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        TIntrusivePtr<TLocalRecoveryInfo> LocalRecoveryInfo;
        TIntrusivePtr<TLsnMngr> LsnMngr;
        TVDiskIncarnationGuid VDiskIncarnationGuid;
        NKikimrVDiskData::TScrubEntrypoint ScrubEntrypoint;
        ui64 ScrubEntrypointLsn;

        TEvLocalRecoveryDone(NKikimrProto::EReplyStatus status,
                             TIntrusivePtr<TLocalRecoveryInfo> recovInfo,
                             std::unique_ptr<NSyncLog::TSyncLogRepaired> repairedSyncLog,
                             std::shared_ptr<NHuge::THullHugeKeeperPersState> repairedHuge,
                             TIntrusivePtr<TSyncerData> syncerData,
                             std::shared_ptr<THullDbRecovery> &&reparedHullDb,
                             const TPDiskCtxPtr &pdiskCtx,
                             TIntrusivePtr<THullCtx> &hullCtx,
                             std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
                             TIntrusivePtr<TLocalRecoveryInfo> &localRecoveryInfo,
                             const TIntrusivePtr<TLsnMngr> &lsnMngr,
                             TVDiskIncarnationGuid vdiskIncarnationGuid,
                             NKikimrVDiskData::TScrubEntrypoint scrubEntrypoint,
                             ui64 scrubEntrypointLsn);
        ~TEvLocalRecoveryDone();
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateDatabaseLocalRecoveryActor
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateDatabaseLocalRecoveryActor(
                const TIntrusivePtr<TVDiskContext> &vctx,
                const TIntrusivePtr<TVDiskConfig> &config,
                const TVDiskID &selfVDiskId,
                const TActorId &skeletonId,
                const TActorId skeletonFrontId,
                std::shared_ptr<TRopeArena> arena);

} // NKikimr

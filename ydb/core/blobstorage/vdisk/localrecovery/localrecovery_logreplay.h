#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    namespace NHuge {
        struct THullHugeKeeperPersState;
    } // NHuge
    namespace NSyncLog {
        struct TSyncLogRepaired;
        class TSyncLogRecovery;
    } // NSyncLog

    struct TSyncerData;
    struct THullCtx;
    class TVDiskContext;
    class THullDbRecovery;
    class TPDiskCtx;
    class TLocalRecoveryInfo;

    ////////////////////////////////////////////////////////////////////////////
    // TLocalRecoveryContext
    ////////////////////////////////////////////////////////////////////////////
    class TLocalRecoveryContext {
    public:
        TIntrusivePtr<TVDiskContext> VCtx;
        TPDiskCtxPtr PDiskCtx;
        NMonGroup::TLocalRecoveryGroup MonGroup;
        TIntrusivePtr<THullCtx> HullCtx;

        TIntrusivePtr<TLocalRecoveryInfo> RecovInfo;
        std::shared_ptr<THullDbRecovery> HullDbRecovery; // for applying recovery log
        TIntrusivePtr<NSyncLog::TSyncLogRecovery> SyncLogRecovery;
        std::shared_ptr<NHuge::THullHugeKeeperPersState> RepairedHuge;
        TIntrusivePtr<TSyncerData> SyncerData;

        // owned chunks as reported by PDisk
        TVector<TChunkIdx> ReportedOwnedChunks;

        TLocalRecoveryContext(TIntrusivePtr<TVDiskContext> vctx);
        ~TLocalRecoveryContext();
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvRecoveryLogReplayDone
    ////////////////////////////////////////////////////////////////////////////
    struct TEvRecoveryLogReplayDone
        : public TEventLocal<TEvRecoveryLogReplayDone, TEvBlobStorage::EvRecoveryLogReplayDone>
    {
        const NKikimrProto::EReplyStatus Status;
        const TString ErrorReason;
        const ui64 RecoveredLsn;

        TEvRecoveryLogReplayDone(NKikimrProto::EReplyStatus status, const TString &errorReason, ui64 recoveredLsn)
            : Status(status)
            , ErrorReason(errorReason)
            , RecoveredLsn(recoveredLsn)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateRecoveryLogReplayer
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateRecoveryLogReplayer(TActorId parentId, std::shared_ptr<TLocalRecoveryContext> locRecCtx);

} // NKikimr

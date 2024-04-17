#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/vdisk/localrecovery/localrecovery_defs.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mon.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_lsnmngr.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_data.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TGuardedActorID
    ////////////////////////////////////////////////////////////////////////////
    class TGuardedActorID {
        TActorId ActorID;
        TAtomic SetUp;

    public:
        TGuardedActorID()
            : ActorID()
            , SetUp(0)
        {}

        void Set(const TActorId &id) {
            ActorID = id;
            AtomicAdd(SetUp, 1);
        }

        operator TActorId() {
            return SetUp ? ActorID : TActorId();
        }

        operator bool() {
            return SetUp;
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // DATABASE
    ////////////////////////////////////////////////////////////////////////////
    class TDb : public TThrRefBase {
        // Basic data
    public:
        TIntrusivePtr<TLsnMngr> LsnMngr = {};

    private:
        // VDiskIncarnationGuid, we generate it on db creation and keep forewer;
        // we can loose it on pdisk crash, i.e. reformat. In this case a new guid
        // will be generated. See details in TVDiskIncarnationGuid definition.
        TVDiskIncarnationGuid VDiskIncarnationGuid;
        bool VDiskIncarnationGuidSet; // only for asserts

    public:
        // SyncLog first item LSN that can't be deleted before unless synced
        std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> SyncLogFirstLsnToKeep;

        // Settings
        TIntrusivePtr<TVDiskConfig> Config;
        TIntrusivePtr<TVDiskContext> VCtx;
        const TBlobStorageGroupType GType;

        TIntrusivePtr<TLocalRecoveryInfo> LocalRecoveryInfo;

        // Actors we are working with
        TGuardedActorID SkeletonID;
        TGuardedActorID LoggerID;
        TGuardedActorID SyncLogID;
        TGuardedActorID SyncerID;
        TGuardedActorID ReplID;
        TGuardedActorID LogCutterID;
        TGuardedActorID HugeKeeperID;
        TGuardedActorID DskSpaceTrackerID;
        TGuardedActorID AnubisRunnerID;

    public:
        void SetVDiskIncarnationGuid(TVDiskIncarnationGuid g) {
            Y_DEBUG_ABORT_UNLESS(!VDiskIncarnationGuidSet);
            VDiskIncarnationGuidSet = true;
            VDiskIncarnationGuid = g;
        }

        TVDiskIncarnationGuid GetVDiskIncarnationGuid(bool allowUnset = false) const {
            Y_DEBUG_ABORT_UNLESS(VDiskIncarnationGuidSet || allowUnset);
            return VDiskIncarnationGuid;
        }

        TDb(TIntrusivePtr<TVDiskConfig> cfg,
            const TVDiskContextPtr &vctx);
        ~TDb();

        friend class TDskSpaceTrackerActor;
    };

} // NKikimr

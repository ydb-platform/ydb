#include "blobstorage_db.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // DATABASE
    ////////////////////////////////////////////////////////////////////////////
    TDb::TDb(TIntrusivePtr<TVDiskConfig> cfg,
             TIntrusivePtr<TBlobStorageGroupInfo> info,
             const TVDiskContextPtr &vctx)
        : VDiskIncarnationGuid()
        , VDiskIncarnationGuidSet(false)
        , SyncLogFirstLsnToKeep(std::make_shared<NSyncLog::TSyncLogFirstLsnToKeep>())
        , Config(cfg)
        , VCtx(vctx)
        , GType(VCtx->Top->GType)
        , LocalRecoveryInfo()
        , Handoff(new THandoffDelegate(vctx->ShortSelfVDisk, info,           // FIXME: we store Info here
                                       THandoffParams{vctx->VDiskLogPrefix,
                                                                 Config->HandoffMaxWaitQueueSize,
                                                                 Config->HandoffMaxWaitQueueByteSize,
                                                                 Config->HandoffMaxInFlightSize,
                                                                 Config->HandoffMaxInFlightByteSize,
                                                                 Config->HandoffTimeout}))
        , SkeletonID()
        , LoggerID()
        , SyncLogID()
        , SyncerID()
        , ReplID()
        , LogCutterID()
        , HugeKeeperID()
        , DskSpaceTrackerID()
        , AnubisRunnerID()
    {}

    TDb::~TDb()
    {}

} // NKikimr

#include "blobstorage_db.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // DATABASE
    ////////////////////////////////////////////////////////////////////////////
    TDb::TDb(TIntrusivePtr<TVDiskConfig> cfg,
             const TVDiskContextPtr &vctx)
        : VDiskIncarnationGuid()
        , VDiskIncarnationGuidSet(false)
        , SyncLogFirstLsnToKeep(std::make_shared<NSyncLog::TSyncLogFirstLsnToKeep>())
        , Config(cfg)
        , VCtx(vctx)
        , GType(VCtx->Top->GType)
        , LocalRecoveryInfo()
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

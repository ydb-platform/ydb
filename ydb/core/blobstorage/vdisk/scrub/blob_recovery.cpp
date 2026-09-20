#include "blob_recovery_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_VDISK_SCRUB

namespace NKikimr {

    void TBlobRecoveryActor::Bootstrap() {
        YDB_LOG_INFO(VDISKP(LogPrefix, "bootstrapping blob recovery actor"),
            {"marker", "VDS27"},
            {"selfId", SelfId()});
        StartQueues();
        Become(&TThis::StateFunc);
    }

    void TBlobRecoveryActor::PassAway() {
        YDB_LOG_INFO(VDISKP(LogPrefix, "blob recovery actor terminating"),
            {"marker", "VDS30"},
            {"selfId", SelfId()});
        StopQueues();
    }

    IActor *CreateBlobRecoveryActor(TIntrusivePtr<TVDiskContext> vctx, TIntrusivePtr<TBlobStorageGroupInfo> info,
            ::NMonitoring::TDynamicCounterPtr counters) {
        return new TBlobRecoveryActor(std::move(vctx), std::move(info), std::move(counters));
    }

} // NKikimr

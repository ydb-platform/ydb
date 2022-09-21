#include "blob_recovery_impl.h"

namespace NKikimr {

    void TBlobRecoveryActor::Bootstrap() {
        STLOG(PRI_INFO, BS_VDISK_SCRUB, VDS27, VDISKP(LogPrefix, "bootstrapping blob recovery actor"), (SelfId, SelfId()));
        StartQueues();
        Become(&TThis::StateFunc);
    }

    void TBlobRecoveryActor::PassAway() {
        STLOG(PRI_INFO, BS_VDISK_SCRUB, VDS30, VDISKP(LogPrefix, "blob recovery actor terminating"), (SelfId, SelfId()));
        StopQueues();
    }

    IActor *CreateBlobRecoveryActor(TIntrusivePtr<TVDiskContext> vctx, TIntrusivePtr<TBlobStorageGroupInfo> info,
            ::NMonitoring::TDynamicCounterPtr counters) {
        return new TBlobRecoveryActor(std::move(vctx), std::move(info), std::move(counters));
    }

} // NKikimr

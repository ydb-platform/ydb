#include "blob_recovery_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

namespace NKikimr {

    void TBlobRecoveryActor::Bootstrap() {
        YDBLOG_COMP_INFO(BS_VDISK_SCRUB, VDISKP(LogPrefix, "bootstrapping blob recovery actor"), {"Marker", "VDS27"},
            {"SelfId", SelfId()});
        StartQueues();
        Become(&TThis::StateFunc);
    }

    void TBlobRecoveryActor::PassAway() {
        YDBLOG_COMP_INFO(BS_VDISK_SCRUB, VDISKP(LogPrefix, "blob recovery actor terminating"), {"Marker", "VDS30"},
            {"SelfId", SelfId()});
        StopQueues();
    }

    IActor *CreateBlobRecoveryActor(TIntrusivePtr<TVDiskContext> vctx, TIntrusivePtr<TBlobStorageGroupInfo> info,
            ::NMonitoring::TDynamicCounterPtr counters) {
        return new TBlobRecoveryActor(std::move(vctx), std::move(info), std::move(counters));
    }

} // NKikimr

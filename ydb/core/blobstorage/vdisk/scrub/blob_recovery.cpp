#include "blob_recovery_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_VDISK_SCRUB

namespace NKikimr {

    void TBlobRecoveryActor::Bootstrap() {
        YDB_LOG_INFO(VDISKP(LogPrefix, "bootstrapping blob recovery actor"),
            {"Marker", "VDS27"},
            {"SelfId", SelfId()});
        StartQueues();
        Become(&TThis::StateFunc);
    }

    void TBlobRecoveryActor::PassAway() {
        YDB_LOG_INFO(VDISKP(LogPrefix, "blob recovery actor terminating"),
            {"Marker", "VDS30"},
            {"SelfId", SelfId()});
        StopQueues();
    }

    IActor *CreateBlobRecoveryActor(TIntrusivePtr<TVDiskContext> vctx, TIntrusivePtr<TBlobStorageGroupInfo> info,
            ::NMonitoring::TDynamicCounterPtr counters) {
        return new TBlobRecoveryActor(std::move(vctx), std::move(info), std::move(counters));
    }

} // NKikimr

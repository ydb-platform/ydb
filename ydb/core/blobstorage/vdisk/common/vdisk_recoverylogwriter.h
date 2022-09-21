#pragma once

#include "defs.h"
#include "vdisk_pdiskctx.h"

namespace NKikimr {

    // Writing via LogWriter makes proper ordering of Lsns
    IActor* CreateRecoveryLogWriter(const TActorId &yardID, const TActorId &skeletonID, NPDisk::TOwner owner,
                                    NPDisk::TOwnerRound ownerRound,
                                    ui64 startLsn, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

} // NKikimr

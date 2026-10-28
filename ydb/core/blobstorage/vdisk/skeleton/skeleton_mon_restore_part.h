#pragma once

#include "defs.h"

#include <ydb/library/actors/core/mon.h>

namespace NKikimr {

    struct TVDiskID;

    IActor* CreateMonRestorePartRequestActor(const TVDiskID& selfVDiskId, const TActorId& notifyId,
        const TActorId& skeletonFrontId, NMon::TEvHttpInfo::TPtr& ev);

} // NKikimr

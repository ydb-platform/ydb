#pragma once
#include "defs.h"

#include <library/cpp/actors/core/mon.h>

namespace NKikimr {

    struct TVDiskID;

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // CreateMonDbMainPageActor -- creates actor for Main Page of a given Hull Database
    ////////////////////////////////////////////////////////////////////////////////////////////////
    IActor *CreateMonDbMainPageActor(
            const TVDiskID &selfVDiskId,
            const TActorId &notifyId,
            const TActorId &skeletonFrontId,
            const TActorId &skeletonId,
            NMon::TEvHttpInfo::TPtr &ev);

} // NKikimr


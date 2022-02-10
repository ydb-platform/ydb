#pragma once

#include "defs.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////
    // dictionary used to determine BS_QUEUE actor id for a specific VDisk inside group
    // (identified by stable TVDiskIdShort)
    using TQueueActorMap = THashMap<TVDiskIdShort, TActorId>;
    using TQueueActorMapPtr = std::shared_ptr<TQueueActorMap>;

} // NKikimr

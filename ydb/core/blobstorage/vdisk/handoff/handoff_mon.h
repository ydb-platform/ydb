#pragma once

#include "defs.h"
#include "handoff_basic.h"

namespace NKikimr {

    IActor *CreateHandoffMonActor(const TVDiskID &selfVDisk,
            std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
            NHandoff::TProxiesPtr proxiesPtr);

} // NKikimr


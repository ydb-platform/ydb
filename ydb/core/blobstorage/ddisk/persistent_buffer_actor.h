#pragma once

#include "defs.h"

#include "ddisk.h"
#include "ddisk_actor.h"
#include "persistent_buffer_space_allocator.h"

namespace NKikimr::NDDisk {

    class TPersistentBufferActor : public TDDiskActor {
    public:
        TPersistentBufferActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
            : TDDiskActor(std::move(baseInfo), info, std::move(pbFormat), std::move(ddiskConfig), counters)
            {
            }
    };

} // NKikimr::NDDisk

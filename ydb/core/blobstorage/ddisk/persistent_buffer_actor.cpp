#include "persistent_buffer_actor.h"

namespace NKikimr::NDDisk {

    IActor *CreatePersistentBufferActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
        return new TPersistentBufferActor(std::move(baseInfo), std::move(info), std::move(pbFormat),
            std::move(ddiskConfig), std::move(counters));
    }

} // NKikimr::NDDisk

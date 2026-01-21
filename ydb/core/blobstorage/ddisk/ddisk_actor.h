#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>

namespace NKikimr::NDDisk {

    class TDDiskActor : public TActorBootstrapped<TDDiskActor> {
        TVDiskConfig::TBaseInfo BaseInfo;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    public:
        TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters);
        void Bootstrap();
        STFUNC(StateFunc);
        void PassAway() override;
    };

} // NKikimr::NDDisk

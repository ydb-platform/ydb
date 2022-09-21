#pragma once
#include "defs.h"

namespace NKikimr {

    struct TVDiskConfig;
    class TBlobStorageGroupInfo;

    IActor* CreateVDisk(const TIntrusivePtr<TVDiskConfig> &cfg,
                        const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                        const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters);

} // NKikimr

#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDiskState,
    ((Unknown)     (0))
    ((Ok)          (1))
    ((Failed)      (2))
    ((RecoverWait) (3))
);

// 1. Remount all disk volumes to it's default state
// 2. Recreate disk layout, all data on disk will be lost
// 3. Replace phisical disk
DEFINE_ENUM(ERecoverPolicy,
    ((RecoverAuto)   (0))
    ((RecoverMount)  (1))
    ((RecoverLayout) (2))
    ((RecoverDisk)   (3))
);

struct TDiskInfo
{
    TString DiskId;
    TString DevicePath;
    TString DeviceName;
    TString DiskModel;
    THashSet<TString> PartitionFsLabels;
    EDiskState State;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TMockedDiskConfig)
DECLARE_REFCOUNTED_STRUCT(TDiskManagerProxyConfig)
DECLARE_REFCOUNTED_STRUCT(TDiskManagerProxyDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TDiskInfoProviderConfig)

DECLARE_REFCOUNTED_STRUCT(IDiskManagerProxy)
DECLARE_REFCOUNTED_CLASS(TDiskInfoProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

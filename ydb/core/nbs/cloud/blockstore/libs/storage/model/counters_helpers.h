#pragma once

#include "disk_description.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

NMonitoring::TDynamicCounterPtr MakeCountersChain(
    NMonitoring::TDynamicCounterPtr counters,
    const TString& ddiskPool,
    const TDiskDescription& diskDescription);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

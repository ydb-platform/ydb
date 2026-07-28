#include "counters_helpers.h"

#include <ydb/core/base/counters.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

NMonitoring::TDynamicCounterPtr MakeCountersChain(
    NMonitoring::TDynamicCounterPtr counters,
    const TString& ddiskPool,
    ui64 tabletId)
{
    if (!counters) {
        return nullptr;
    }

    NMonitoring::TDynamicCounterPtr result =
        NKikimr::GetServiceCounters(std::move(counters), "nbs_partitions");
    result = result->GetSubgroup("ddiskPool", ddiskPool);
    result = result->GetSubgroup("tabletId", ToString(tabletId));
    result = result->GetSubgroup("subsystem", "interface");
    return result;
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

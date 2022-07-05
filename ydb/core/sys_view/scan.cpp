#include "scan.h"

#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/partition_stats/partition_stats.h>
#include <ydb/core/sys_view/nodes/nodes.h>
#include <ydb/core/sys_view/query_stats/query_stats.h>
#include <ydb/core/sys_view/query_stats/query_metrics.h>
#include <ydb/core/sys_view/storage/pdisks.h>
#include <ydb/core/sys_view/storage/vslots.h>
#include <ydb/core/sys_view/storage/groups.h>
#include <ydb/core/sys_view/storage/storage_pools.h>
#include <ydb/core/sys_view/storage/storage_stats.h>
#include <ydb/core/sys_view/tablets/tablets.h>
#include <ydb/core/sys_view/partition_stats/top_partitions.h>

namespace NKikimr {
namespace NSysView {

THolder<IActor> CreateSystemViewScan(const TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    if (tableId.SysViewInfo == PartitionStatsName) {
        return CreatePartitionStatsScan(ownerId, scanId, tableId, tableRange, columns);
    }

    if (tableId.SysViewInfo == NodesName) {
        return CreateNodesScan(ownerId, scanId, tableId, tableRange, columns);
    }

    if (tableId.SysViewInfo == TopQueriesByDuration1MinuteName ||
        tableId.SysViewInfo == TopQueriesByDuration1HourName ||
        tableId.SysViewInfo == TopQueriesByReadBytes1MinuteName ||
        tableId.SysViewInfo == TopQueriesByReadBytes1HourName ||
        tableId.SysViewInfo == TopQueriesByCpuTime1MinuteName ||
        tableId.SysViewInfo == TopQueriesByCpuTime1HourName ||
        tableId.SysViewInfo == TopQueriesByRequestUnits1MinuteName ||
        tableId.SysViewInfo == TopQueriesByRequestUnits1HourName)
    {
        return CreateQueryStatsScan(ownerId, scanId, tableId, tableRange, columns);
    }

    if (tableId.SysViewInfo == PDisksName) {
        return CreatePDisksScan(ownerId, scanId, tableId, tableRange, columns);
    }

    if (tableId.SysViewInfo == VSlotsName) {
        return CreateVSlotsScan(ownerId, scanId, tableId, tableRange, columns);
    }

    if (tableId.SysViewInfo == GroupsName) {
        return CreateGroupsScan(ownerId, scanId, tableId, tableRange, columns);
    }

    if (tableId.SysViewInfo == StoragePoolsName) {
        return CreateStoragePoolsScan(ownerId, scanId, tableId, tableRange, columns);
    }

    if (tableId.SysViewInfo == StorageStatsName) {
        return CreateStorageStatsScan(ownerId, scanId, tableId, tableRange, columns);
    }

    if (tableId.SysViewInfo == TabletsName) {
        return CreateTabletsScan(ownerId, scanId, tableId, tableRange, columns);
    }

    if (tableId.SysViewInfo == QueryMetricsName) {
        return CreateQueryMetricsScan(ownerId, scanId, tableId, tableRange, columns);
    }

    if (tableId.SysViewInfo == TopPartitions1MinuteName ||
        tableId.SysViewInfo == TopPartitions1HourName)
    {
        return CreateTopPartitionsScan(ownerId, scanId, tableId, tableRange, columns);
    }

    return {};
}

} // NSysView
} // NKikimr

#pragma once

#include "ydb_metrics_aggregator.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>

namespace NKikimr {

// Aggregator for all database metrics on a single node.
class TNodeDatabaseMetricsAggregator : public TThrRefBase {
    static TString FakeDatabasePath(const TPathId& tenantPathId) {
        // There is no easy way to get a real database path. Use a fake path constructed from the tenant path ID.
        return TStringBuilder() << tenantPathId.OwnerId << "-" << tenantPathId.LocalPathId;
    }
public:

    TNodeDatabaseMetricsAggregator(const TPathId& tenantPathId, // Tenant path ID for the database
        NMonitoring::TDynamicCounterPtr parentCounterGroup)     // The parent group to store counters for all databases to
        : TenantPathId(tenantPathId)
        , DatabasePath(FakeDatabasePath(tenantPathId))
        , ParentCounterGroup(parentCounterGroup)
        , DatabaseCounterGroup(parentCounterGroup->GetSubgroup("database", DatabasePath))
    {
    }

    // Delete all counters for the database.
    void DeleteAllDatabaseCounters() {
        ParentCounterGroup->RemoveSubgroup("database", DatabasePath);
    }

    // Process tablet counters and apply them to the corresponding metrics counters.
    void ProcessTabletCounters(TEvTabletCounters::TEvTabletAddCounters* message);

    // Remove the values supplied by the specified tablet from all counters.
    void ForgetTablet(ui64 tabletId, ui32 followerId);

private:

    // A partition replica detailed metrics.
    struct TPartitionReplicaMetricsInfo {
        // Counter group for this follower or leader. Public YDB metrics (table.datashard.*) are created directly in this counter group.
        // Low level metrics for each tablet type are created in child subgroups, e.g. "type"="DataShard" for a DataShard tablets.
        NMonitoring::TDynamicCounterPtr ReplicaCounterGroup;

        // Low-level counters processor. Translates counter values from tablet to the actual metrics counters for the ReplicaCounterGroup.
        ITabletCountersProcessorPtr TabletCountersProcessor;

        // Mapper to translate tablet type dependant counters from corresponding type subgroup to the ReplicaCounterGroup counters.
        TYdbMetricsMapperPtr TabletYdbMetricsMapper;
    };

    // Detailed metrics for a partition, e.g. all partition replicas.
    struct TPartitionMetricsInfo {
        // High-level counter group for a partition. Contains aggregated values across all replicas, has "tablet_id" = "<table_id>" label.
        NMonitoring::TDynamicCounterPtr PartitionCounterGroup;

        // High-level metrics aggregator for a partition.
        TYdbMetricsAggregatorPtr PartitionYdbMetricsAggregator;

        // High-level counter group for followers. Corresponds to the "follower_id" = "replicas_only" label.
        NMonitoring::TDynamicCounterPtr AllFollowersCounterGroup;

        // High-level metrics aggregator for followers.
        TYdbMetricsAggregatorPtr AllFollowersYdbMetricsAggregator;

        // Detailed metrics for each of replicas.
        std::unordered_map<ui32, TPartitionReplicaMetricsInfo> PerReplicaMetrics;
    };

    // Detailed metrics for a table.
    struct TTableMetricsInfo {
        // Most recent detailed metrics configuration for the table, e.g. with highest TTableMetricsConfig::TableSchemaVersion field.
        TEvTabletCounters::TTableMetricsConfig TableMetricsConfig;

        // Tablet type of all partition replicas.
        TTabletTypes::EType PartitionReplicaTabletType;

        // Counter group for all high-level counters for the table, e.g. labeled "table" = "<table_path>".
        NMonitoring::TDynamicCounterPtr TableCounterGroup;

        // Group with "monitoring_project_id" = "<project_id>" label used as a parent for TableCounterGroup if TTableMetricsConfig::MonitoringProjectId
        // field is set. DatabaseCounterGroup used otherwise.
        NMonitoring::TDynamicCounterPtr MonitoringProjectCounterGroup;

        // The counter group, which contains all high-level counters grouped by the partition ID.
        NMonitoring::TDynamicCounterPtr PerPartitionCounterGroup;

        // The aggregator for the high-level table metrics. Aggregates metrics from all replicas of all table partitions (from PerPartitionMetrics).
        TYdbMetricsAggregatorPtr PerPartitionTableYdbMetricsAggregator;

        // Table partion metrics indexed by tablet ID.
        std::unordered_map<ui64, TPartitionMetricsInfo> PerPartitionMetrics;
    };

    // Track schema version changes for a table.
    void HandleTableSchemaVersionChange(std::unordered_map<ui64, TTableMetricsInfo>::iterator tableMetricsIt,
        TEvTabletCounters::TEvTabletAddCounters* message);

    // Track schema version changes for a tenant database.
    void HandleDatabaseSchemaVersionChange(std::unordered_map<ui64, TTableMetricsInfo>::iterator tableMetricsIt,
        TEvTabletCounters::TEvTabletAddCounters* message);

    const TPathId TenantPathId;                             // Tenant path ID for the database
    TString DatabasePath;                                   // Database path, see FakeDatabasePath
    NMonitoring::TDynamicCounterPtr ParentCounterGroup;     // Parent group for counters for all databases
    NMonitoring::TDynamicCounterPtr DatabaseCounterGroup;   // Group for aggregated database counters, labeled "database" = "<database_path>".
    std::unordered_map<ui64, TTableMetricsInfo> PerTableMetrics;    // Metrics for each table in the database.

    struct TabletIdFollowerIdPairHash {
        std::size_t operator()(const std::pair<ui64, ui32>& p) const {
            return std::hash<ui64>{}(p.first) ^ std::hash<ui32>{}(p.second);
        }
    };

    std::unordered_map<std::pair<ui64, ui32>, ui64, TabletIdFollowerIdPairHash> TabletIdFollowerIdToTableIdMap; // {tablet ID + follower ID} -> table ID
};

using TNodeDatabaseMetricsAggregatorPtr = TIntrusivePtr<TNodeDatabaseMetricsAggregator>;

} // namespace NKikimr

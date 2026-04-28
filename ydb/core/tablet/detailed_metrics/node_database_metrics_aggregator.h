#pragma once

#include "ydb_metrics_aggregator.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>

namespace NKikimr {

/**
 * The aggregator for all metrics for the given database for the given node.
 */
class TNodeDatabaseMetricsAggregator : public TThrRefBase {
public:
    /**
     * The constructor, which creates the counter group for all metrics
     * aggregated for the given database.
     *
     * @param[in] tenantPathId The tenant path ID for the database for the given aggregator
     * @param[in] parentCounterGroup The parent group, where counters for all databases are stored
     */
    TNodeDatabaseMetricsAggregator(
        const TPathId& tenantPathId,
        NMonitoring::TDynamicCounterPtr parentCounterGroup
    ) : TenantPathId(tenantPathId)
      , DatabasePath(
            // NOTE: There is no easy way to resolve the tenant path ID to the corresponding
            //       database path. When the aggregator starts, use a fake path constructed
            //       from the tenant path ID. Once the database path is resolved, the corresponding
            //       counter groups will be updated to use the correct "database" label.
            TStringBuilder()
                << tenantPathId.OwnerId
                << "-"
                << tenantPathId.LocalPathId
        )
      , ParentCounterGroup(parentCounterGroup)
      , DatabaseCounterGroup(parentCounterGroup->GetSubgroup("database", DatabasePath))
    {
    }

    /**
     * Delete all counters for the given database.
     */
    void DeleteAllDatabaseCounters() {
        ParentCounterGroup->RemoveSubgroup("database", DatabasePath);
    }

    /**
     * Process a single batch of tablet counters and apply them to the corresponding
     * metrics counters.
     *
     * @note This function assumes that the given counters are differential
     *       (as a delta) from the previous batch.
     *
     * @param[in] message The TEvTabletAddCounters message with the batch to process
     */
    void ProcessTabletCounters(TEvTabletCounters::TEvTabletAddCounters* message);

    /**
     * Remove the values supplied by the given tablet from all counters.
     *
     * @param[in] tabletId The ID of the tablet
     * @param[in] followerId The ID of the follower
     */
    void ForgetTablet(ui64 tabletId, ui32 followerId);

private:
    /**
     * The container for all the pieces needed to track detailed metrics
     * for the given follower (or the leader) for the given partition.
     */
    struct TPartitionReplicaMetricsInfo {
        /**
         * The counter group, which contains all counters (both high-level and low-level)
         * for the given follower/leader.
         *
         * @note Public YDB metrics (for example, table.datashard.*) are created
         *       directly in this counter group. Low level metrics for each tablet type
         *       are created in child subgroups. For example, low level metrics
         *       for Data Shard tablets are created in the subgroup labelled
         *       as "type"="DataShard".
         *
         * @note This corresponds to the "follower_id" = "<follower_id>" label.
         */
        NMonitoring::TDynamicCounterPtr ReplicaCounterGroup;

        /**
         * The processor for low-level counters for the given replica.
         *
         * @note This processor is responsible for translating the counter values
         *       coming directly from the tablet (see TEvTabletAddCounters)
         *       into the actual metrics counters stored inside the corresponding
         *       subgroup (e.g., the one labelled as "type"="DataShard") inside
         *       the ReplicaCounterGroup counter group.
         */
        ITabletCountersProcessorPtr TabletCountersProcessor;

        /**
         * The mapper from the low-level metrics for the given replica
         * to the corresponding high-level metrics (for example, table.datashard.*).
         *
         * @note This mapper translates the low level metrics from the corresponding
         *       type subgroup directly into the the ReplicaCounterGroup counter group.
         */
        TYdbMetricsMapperPtr TabletYdbMetricsMapper;
    };

    /**
     * The container for all the pieces needed to track detailed metrics
     * for the given partition.
     */
    struct TPartitionMetricsInfo {
        /**
         * The counter group, which contains all high-level counters for the given partition.
         *
         * @note Public YDB metrics (for example, table.datashard.*) are created
         *       directly in this counter group. These counters contain aggregated
         *       values across all replicas of the given partition.
         *
         * @note This corresponds to the "tablet_id" = "<table_id>" label.
         */
        NMonitoring::TDynamicCounterPtr PartitionCounterGroup;

        /**
         * The aggregator for the high-level metrics for the given partition.
         */
        TYdbMetricsAggregatorPtr PartitionYdbMetricsAggregator;

        /**
         * The counter group, which contains all high-level counters
         * aggregated across all followers (excluding the leader) for the given partition.
         *
         * @note This corresponds to the "follower_id" = "replicas_only" label.
         */
        NMonitoring::TDynamicCounterPtr AllFollowersCounterGroup;

        /**
         * The aggregator for the high-level metrics for all followers (excluding the leader)
         * for the given partition.
         */
        TYdbMetricsAggregatorPtr AllFollowersYdbMetricsAggregator;

        /**
         * The list of metrics info-containers for each replica (including the leader)
         * for the given partition.
         *
         * @note The key here is the follower ID (0 == the leader).
         */
        std::unordered_map<ui32, TPartitionReplicaMetricsInfo> PerReplicaMetrics;
    };

    /**
     * The container for all the pieces needed to track detailed metrics
     * for the given table.
     */
    struct TTableMetricsInfo {
        /**
         * The last known detailed metrics configuration for the given table.
         *
         * @note This information is transmitted in each TEvTabletAddCounters message
         *       by each tablet. The TTableMetricsConfig::SchemaVersion field
         *       is used to resolve conflicts and choose the most recent configuration.
         */
        TEvTabletCounters::TTableMetricsConfig TableMetricsConfig;

        /**
         * The type for all tablets (both followers and leaders) holding the data
         * for all partitions the given table.
         *
         * @warning This assumes that all followers/leaders for all partitions
         *          of the given table are of the same type. In other words,
         *          per-replica per-partition detailed metrics can be coming
         *          from tables only of this type. This does not apply
         *          to the detailed metrics for the entire table - they can be coming
         *          from tablets of different types (multiple).
         */
        TTabletTypes::EType PartitionReplicaTabletType;

        /**
         * The counter group, which contains all high-level counters for the given table.
         *
         * @note Public YDB metrics (for example, table.datashard.*) are created
         *       directly in this counter group. These counters contain aggregated
         *       values across all partitions of the given table.
         *
         * @note This corresponds to the "table" = "<table_path>" label.
         */
        NMonitoring::TDynamicCounterPtr TableCounterGroup;

        /**
         * The counter group, which holds the Monitoring project ID for all detailed metrics
         * for the given table.
         *
         * @note If the TTableMetricsConfig::MonitoringProjectId field is set,
         *       then this counter group is the parent group for the TableCounterGroup group.
         *       If this field is not set, then the TableCounterGroup group
         *       is created directly in the parent counter group for the given
         *       database (see DatabaseCounterGroup).
         *
         * @note This corresponds to the "monitoring_project_id" = "<project_id>" label.
         */
        NMonitoring::TDynamicCounterPtr MonitoringProjectCounterGroup;

        /**
         * The counter group, which contains all high-level counters grouped
         * by the partition ID.
         *
         * @note This corresponds to the "detailed_metrics" = "per_partition" label.
         */
        NMonitoring::TDynamicCounterPtr PerPartitionCounterGroup;

        /**
         * The aggregator for the high-level metrics for the given table.
         *
         * @note This aggregator is responsible aggregating high-level metrics
         *       from all replicas of all partitions for the given table
         *       (from PerPartitionMetrics). Per-table metrics are aggregated
         *       separately.
         */
        TYdbMetricsAggregatorPtr PerPartitionTableYdbMetricsAggregator;

        /**
         * The list of metrics info-containers for each partition for the given table.
         *
         * @note The key here is the partition ID (as TabletId).
         */
        std::unordered_map<ui64, TPartitionMetricsInfo> PerPartitionMetrics;
    };

    /**
     * Check, if the schema version for the given table has changed and,
     * if necessary, perform the necessary actions to handle the changes.
     *
     * @param[in] tableMetricsIt The iterator for the table metrics entry
     * @param[in] message The TEvTabletAddCounters message (may contain the new schema version)
     */
    void HandleTableSchemaVersionChange(
        std::unordered_map<ui64, TTableMetricsInfo>::iterator tableMetricsIt,
        TEvTabletCounters::TEvTabletAddCounters* message
    );

    /**
     * Check, if the schema version for the given tenant database has changed and,
     * if necessary, perform the necessary actions to handle the changes.
     *
     * @param[in] tableMetricsIt The iterator for the table metrics entry
     * @param[in] message The TEvTabletAddCounters message (may contain the new schema version)
     */
    void HandleDatabaseSchemaVersionChange(
        std::unordered_map<ui64, TTableMetricsInfo>::iterator tableMetricsIt,
        TEvTabletCounters::TEvTabletAddCounters* message
    );

    /**
     * The tenant path ID for the database, which is handled by the given aggregator.
     */
    const TPathId TenantPathId;

    /**
     * The database path for the database, which is handled by the given aggregator.
     *
     * @warning Until the database path is resolved, this is constructed from TenantPathId.
     */
    TString DatabasePath;

    /**
     * The parent group, where counters for all databases are stored
     */
    NMonitoring::TDynamicCounterPtr ParentCounterGroup;

    /**
     * The counter group where all the aggregated counters for the given database will be created.
     *
     * @note This corresponds to the "database" = "<database_path>" label.
     */
    NMonitoring::TDynamicCounterPtr DatabaseCounterGroup;

    /**
     * The list of metrics info-containers for each table for the given database.
     *
     * @note The key here is the table ID (as PathId).
     */
    std::unordered_map<ui64, TTableMetricsInfo> PerTableMetrics;

    /**
     * The custom hash calculator, which allows using std::pair<ui64, ui32>
     * as a key in standard maps and sets.
     */
    struct TabletIdFollowerIdPairHash {
        std::size_t operator()(const std::pair<ui64, ui32>& p) const {
            return std::hash<ui64>{}(p.first) ^ std::hash<ui32>{}(p.second);
        }
    };

    /**
     * The reverse lookup map, which provides a fast mechanism to figure out,
     * which table the given tablet ID + follower ID pair belongs to.
     *
     * @note The key in this map is the {TabletId, FollowerId} pair.
     *       And the value is the corresponding TableId.
     */
    std::unordered_map<
        std::pair<ui64, ui32>,
        ui64,
        TabletIdFollowerIdPairHash
    > TabletIdFollowerIdToTableIdMap;
};

using TNodeDatabaseMetricsAggregatorPtr = TIntrusivePtr<TNodeDatabaseMetricsAggregator>;

} // namespace NKikimr

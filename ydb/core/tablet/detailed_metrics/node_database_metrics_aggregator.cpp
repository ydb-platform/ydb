#include "node_database_metrics_aggregator.h"

namespace NKikimr {

void TNodeDatabaseMetricsAggregator::HandleDatabaseSchemaVersionChange(std::unordered_map<ui64, TTableMetricsInfo>::iterator tableMetricsIt,
    TEvTabletCounters::TEvTabletAddCounters* message)
{
    if (message->TableMetricsConfig->TenantDbSchemaVersion == tableMetricsIt->second.TableMetricsConfig.TenantDbSchemaVersion) {
        return;
    }

    auto messageTail = [&] () -> TString { return TStringBuilder() <<
        " (update received from the tablet ID " << message->TabletID << ", " <<
        "follower ID " << message->FollowerId << " (" <<
        "type " << TTabletTypes::TypeToStr(message->TabletType) << "), " <<
        "tenant database schema version " << message->TableMetricsConfig->TenantDbSchemaVersion << ") " <<
        "for the table " << tableMetricsIt->second.TableMetricsConfig.TablePath << " (" <<
        "table ID " << tableMetricsIt->second.TableMetricsConfig.TableId << "), " <<
        "tenant database " << TenantPathId;
    };

    if (message->TableMetricsConfig->TenantDbSchemaVersion < tableMetricsIt->second.TableMetricsConfig.TenantDbSchemaVersion) {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
            "Ignoring outdated detailed metrics configuration, current schema version " <<
            tableMetricsIt->second.TableMetricsConfig.TenantDbSchemaVersion << messageTail());

        return;
    }

    if (message->TableMetricsConfig->MonitoringProjectId != tableMetricsIt->second.TableMetricsConfig.MonitoringProjectId) {
        if (!tableMetricsIt->second.TableMetricsConfig.MonitoringProjectId) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
                "Adding the monitoring project ID " << (*message->TableMetricsConfig->MonitoringProjectId) << messageTail());

            tableMetricsIt->second.MonitoringProjectCounterGroup = DatabaseCounterGroup->GetSubgroup("monitoring_project_id",
                *message->TableMetricsConfig->MonitoringProjectId);

            DatabaseCounterGroup->RemoveSubgroup("table", tableMetricsIt->second.TableMetricsConfig.TablePath);

            tableMetricsIt->second.MonitoringProjectCounterGroup->RegisterSubgroup("table", tableMetricsIt->second.TableMetricsConfig.TablePath,
                tableMetricsIt->second.TableCounterGroup);
        } else if (!message->TableMetricsConfig->MonitoringProjectId) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
                "Removing the monitoring project ID " << (*tableMetricsIt->second.TableMetricsConfig.MonitoringProjectId) << messageTail());

            DatabaseCounterGroup->RegisterSubgroup("table", tableMetricsIt->second.TableMetricsConfig.TablePath,
                tableMetricsIt->second.TableCounterGroup);

            tableMetricsIt->second.MonitoringProjectCounterGroup->RemoveSubgroup("table", tableMetricsIt->second.TableMetricsConfig.TablePath);

            tableMetricsIt->second.MonitoringProjectCounterGroup.Reset();
        } else {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
                "Changing the monitoring project ID from " << (*tableMetricsIt->second.TableMetricsConfig.MonitoringProjectId) <<
                " to " << (*message->TableMetricsConfig->MonitoringProjectId) << messageTail());

            tableMetricsIt->second.MonitoringProjectCounterGroup->RemoveSubgroup("table", tableMetricsIt->second.TableMetricsConfig.TablePath);

            tableMetricsIt->second.MonitoringProjectCounterGroup = DatabaseCounterGroup->GetSubgroup("monitoring_project_id",
                *message->TableMetricsConfig->MonitoringProjectId);

            tableMetricsIt->second.MonitoringProjectCounterGroup->RegisterSubgroup("table", tableMetricsIt->second.TableMetricsConfig.TablePath,
                tableMetricsIt->second.TableCounterGroup);
        }
    } else {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
            "Not changing the monitoring project ID " <<
            tableMetricsIt->second.TableMetricsConfig.MonitoringProjectId.value_or("NOT_SPECIFIED") << messageTail());
    }

    tableMetricsIt->second.TableMetricsConfig.TenantDbSchemaVersion = message->TableMetricsConfig->TenantDbSchemaVersion;
    tableMetricsIt->second.TableMetricsConfig.MonitoringProjectId = message->TableMetricsConfig->MonitoringProjectId;
}


void TNodeDatabaseMetricsAggregator::HandleTableSchemaVersionChange(std::unordered_map<ui64, TTableMetricsInfo>::iterator tableMetricsIt,
    TEvTabletCounters::TEvTabletAddCounters* message)
{
    if (message->TableMetricsConfig->TableSchemaVersion == tableMetricsIt->second.TableMetricsConfig.TableSchemaVersion) {
        return;
    }

    if (message->TableMetricsConfig->TableSchemaVersion < tableMetricsIt->second.TableMetricsConfig.TableSchemaVersion) {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
            "Ignoring the detailed metrics configuration (received table schema version " << message->TableMetricsConfig->TableSchemaVersion << ", " <<
            "the current known table schema version " << tableMetricsIt->second.TableMetricsConfig.TableSchemaVersion << ") " <<
            "received from the tablet ID " << message->TabletID << " (" <<
            "type " << TTabletTypes::TypeToStr(message->TabletType) << "), " <<
            "table " << tableMetricsIt->second.TableMetricsConfig.TablePath << " (" <<
            "table ID " << tableMetricsIt->second.TableMetricsConfig.TableId << "), " <<
            "tenant database " << TenantPathId);

        return;
    }

    Y_ABORT_UNLESS(message->TableMetricsConfig->TablePath == tableMetricsIt->second.TableMetricsConfig.TablePath,
        "The table path for the table ID %" PRIu64 " changed from %s to %s when the table schema version changed from %" PRIu64 " to %" PRIu64,
        tableMetricsIt->second.TableMetricsConfig.TableId, tableMetricsIt->second.TableMetricsConfig.TablePath.c_str(),
        message->TableMetricsConfig->TablePath.c_str(), tableMetricsIt->second.TableMetricsConfig.TableSchemaVersion,
        message->TableMetricsConfig->TableSchemaVersion);
}

void TNodeDatabaseMetricsAggregator::ProcessTabletCounters(TEvTabletCounters::TEvTabletAddCounters* message) {
    if (!message->TableMetricsConfig) {
        return;
    }

    switch (message->TabletType) {
    case TTabletTypes::DataShard:
        break;
    default: // Process detailed metrics only from known types
        return;
    }

    auto tableMetricsIt = PerTableMetrics.find(message->TableMetricsConfig->TableId);
    if (tableMetricsIt == PerTableMetrics.end()) {
        // This is a new entry for a new table, process detailed metrics only if the metrics level is above DATABASE
        switch (message->TableMetricsConfig->MetricsLevel) {
        case NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable:
        case NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition:
            break;
        default:
            return;
        }

        LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
            "Creating a new entry for detailed metrics for the table " << message->TableMetricsConfig->TablePath << " (" <<
            "table ID " << message->TableMetricsConfig->TableId << "), " <<
            "metrics level " << NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel_Name(message->TableMetricsConfig->MetricsLevel) << ", " <<
            "tenant database " << TenantPathId << ", " <<
            "monitoring project ID " << message->TableMetricsConfig->MonitoringProjectId.value_or("NOT_SPECIFIED"));

        // Place all table metrics into a subgroup with the "monitoring_project_id" label, if the monitoring project ID is known
        auto parentCounterGroup = DatabaseCounterGroup;
        NMonitoring::TDynamicCounterPtr monitoringProjectCounterGroup;

        if (message->TableMetricsConfig->MonitoringProjectId) {
            monitoringProjectCounterGroup = DatabaseCounterGroup->GetSubgroup("monitoring_project_id",
                *message->TableMetricsConfig->MonitoringProjectId);
            parentCounterGroup = monitoringProjectCounterGroup;
        }

        auto tableCounterGroup = parentCounterGroup->GetSubgroup("table",
            message->TableMetricsConfig->TablePath);

        // Allocate a new entry for this new table
        tableMetricsIt = PerTableMetrics.emplace(std::piecewise_construct,
            std::forward_as_tuple(message->TableMetricsConfig->TableId),
            std::forward_as_tuple(TTableMetricsInfo{
                .TableMetricsConfig = *message->TableMetricsConfig,
                .PartitionReplicaTabletType = message->TabletType,
                .TableCounterGroup = tableCounterGroup,
                .MonitoringProjectCounterGroup = monitoringProjectCounterGroup,
                .PerPartitionCounterGroup = tableCounterGroup->GetSubgroup("detailed_metrics", "per_partition"),
                .PerPartitionTableYdbMetricsAggregator = CreateYdbMetricsAggregatorByTabletType(message->TabletType,
                    tableCounterGroup),
            })
        ).first;
    } else {
        // This is an existing table, make sure all tablets have the same type
        Y_ABORT_UNLESS(message->TabletType == tableMetricsIt->second.PartitionReplicaTabletType,
            "The tablet ID %" PRIu64 " (type %s) of the table %s (table ID %" PRIu64 ") uses an inconsistent type (expected %s)",
            message->TabletID, TTabletTypes::TypeToStr(message->TabletType), tableMetricsIt->second.TableMetricsConfig.TablePath.c_str(),
            tableMetricsIt->second.TableMetricsConfig.TableId, TTabletTypes::TypeToStr(tableMetricsIt->second.PartitionReplicaTabletType));

        /**
         * @todo Once per-table metrics are added (from Scheme Shard), the code here should be changed to verify that the tablet type for per-partition
         *       metrics (should be Data Shard or Column Shard) is different from the tablet type for per-table metrics (should be Scheme Shard).
         *       If the same tablet type is used for reporting both per-partition and per-table metrics, the code in this class as it is written now
         *       will get confused and will lose metrics values - the counters at the table level will get overwritten by per-table and per-partition
         *       metrics.
         */

        HandleDatabaseSchemaVersionChange(tableMetricsIt, message);
        HandleTableSchemaVersionChange(tableMetricsIt, message);
    }

    // Add a new partition entry, if this partition has never been seen before
    auto partitionMetricsIt = tableMetricsIt->second.PerPartitionMetrics.find(message->TabletID);
    if (partitionMetricsIt == tableMetricsIt->second.PerPartitionMetrics.end()) {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
            "Creating a new entry for detailed metrics for the tablet ID " << message->TabletID << " (" <<
            "type " << TTabletTypes::TypeToStr(message->TabletType) << "), " <<
            "table " << tableMetricsIt->second.TableMetricsConfig.TablePath << " (" <<
            "table ID " << tableMetricsIt->second.TableMetricsConfig.TableId << "), " <<
            "tenant database " << TenantPathId);

        auto partitionCounterGroup = tableMetricsIt->second.PerPartitionCounterGroup->GetSubgroup("tablet_id",
            ToString(message->TabletID));

        auto allFollowersCounterGroup = partitionCounterGroup->GetSubgroup("follower_id",
            "replicas_only");

        // Allocate a new entry for this new partition
        partitionMetricsIt = tableMetricsIt->second.PerPartitionMetrics.emplace(std::piecewise_construct,
            std::forward_as_tuple(message->TabletID),
            std::forward_as_tuple(TPartitionMetricsInfo{
                .PartitionCounterGroup = partitionCounterGroup,
                .PartitionYdbMetricsAggregator = CreateYdbMetricsAggregatorByTabletType(message->TabletType,
                    partitionCounterGroup),
                .AllFollowersCounterGroup = allFollowersCounterGroup,
                .AllFollowersYdbMetricsAggregator = CreateYdbMetricsAggregatorByTabletType(message->TabletType,
                    allFollowersCounterGroup),
            })
        ).first;

        // Connect the metrics aggregators for this partition to the corresponding parents
        partitionMetricsIt->second.PartitionYdbMetricsAggregator->AddSourceCountersGroup("replicas_only",
            allFollowersCounterGroup);

        tableMetricsIt->second.PerPartitionTableYdbMetricsAggregator->AddSourceCountersGroup(ToString(message->TabletID),
            partitionCounterGroup
        );
    }

    // Add a new replica entry, if this follower/leader has never been seen before
    auto replicaMetricsIt = partitionMetricsIt->second.PerReplicaMetrics.find(message->FollowerId);
    if (replicaMetricsIt == partitionMetricsIt->second.PerReplicaMetrics.end()) {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
            "Creating a new entry for detailed metrics for the follower ID " << message->FollowerId << ", " <<
            "tablet ID " << message->TabletID << " (" <<
            "type " << TTabletTypes::TypeToStr(message->TabletType) << "), " <<
            "table " << tableMetricsIt->second.TableMetricsConfig.TablePath << " (" <<
            "table ID " << tableMetricsIt->second.TableMetricsConfig.TableId << "), " <<
            "tenant database " << TenantPathId);

        auto inserted = TabletIdFollowerIdToTableIdMap.emplace(std::make_pair(message->TabletID, message->FollowerId),
            tableMetricsIt->second.TableMetricsConfig.TableId);

        Y_ABORT_UNLESS(inserted.second,
            "The combination of the tablet ID %" PRIu64 " (type %s) and the follower ID %" PRIu32 " is already present for the table ID %" PRIu64
            " when adding a new entry for detailed metrics for the table %s (table ID %" PRIu64 ")",
            message->TabletID, TTabletTypes::TypeToStr(message->TabletType), message->FollowerId, inserted.first->second,
            tableMetricsIt->second.TableMetricsConfig.TablePath.c_str(), tableMetricsIt->second.TableMetricsConfig.TableId);

        auto replicaCounterGroup = partitionMetricsIt->second.PartitionCounterGroup->GetSubgroup("follower_id",
            ToString(message->FollowerId));

        replicaMetricsIt = partitionMetricsIt->second.PerReplicaMetrics.emplace(std::piecewise_construct,
            std::forward_as_tuple(message->FollowerId),
            std::forward_as_tuple(TPartitionReplicaMetricsInfo{
                .ReplicaCounterGroup = replicaCounterGroup,
                .TabletCountersProcessor = CreateTabletCountersProcessor(replicaCounterGroup, message->TabletType),
                .TabletYdbMetricsMapper = CreateYdbMetricsMapperByTabletType(message->TabletType,
                    replicaCounterGroup, replicaCounterGroup),
            })
        ).first;

        // Connect the metrics aggregators for this partition to the corresponding parents
        if (message->FollowerId == 0) {
            // The leader goes directly to the partition metrics
            partitionMetricsIt->second.PartitionYdbMetricsAggregator->AddSourceCountersGroup(ToString(message->FollowerId),
                replicaCounterGroup);
        } else {
            // All followers go to the "replicas_only" group and then (indirectly) to the partition metrics
            partitionMetricsIt->second.AllFollowersYdbMetricsAggregator->AddSourceCountersGroup(ToString(message->FollowerId),
                replicaCounterGroup);
        }
    }

    // Update the received metrics and all aggregates. The TTabletCountersForTabletType class (the one, which really implements
    // the ITabletCountersProcessor interface) is written in such a way, that the ProcessTabletCounters() function updates only the values
    // of direct counters, which derive their values directly from the tablet counters. Any aggregate values (e.g. "SUM(DbUniqueRowsTotal)"
    // or "HIST(ConsumedCPU)") are not updated. To see the updated values, the RecalculateAggregatedValues() function must be called explicitly.
    replicaMetricsIt->second.TabletCountersProcessor->ProcessTabletCounters(message->TabletID,
        message->ExecutorCounters.Get(),
        message->AppCounters.Get(),
        message->TabletType);

    replicaMetricsIt->second.TabletCountersProcessor->RecalculateAggregatedValues();
    replicaMetricsIt->second.TabletYdbMetricsMapper->TransferCounterValues();

    if (message->FollowerId != 0) {
        partitionMetricsIt->second.AllFollowersYdbMetricsAggregator->RecalculateAllTargetCounters();
    }

    partitionMetricsIt->second.PartitionYdbMetricsAggregator->RecalculateAllTargetCounters();
    tableMetricsIt->second.PerPartitionTableYdbMetricsAggregator->RecalculateAllTargetCounters();
}

void TNodeDatabaseMetricsAggregator::ForgetTablet(ui64 tabletId, ui32 followerId) {
    auto lookupIt = TabletIdFollowerIdToTableIdMap.find(std::make_pair(tabletId, followerId));

    if (lookupIt == TabletIdFollowerIdToTableIdMap.end()) {
        LOG_WARN_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
            "Not deleting the entry for detailed metrics for an unknown tablet ID " << tabletId << " (" <<
            "requested follower ID " << followerId << "), " <<
            "tenant database " << TenantPathId);
        return;
    }

    auto tableMetricsIt = PerTableMetrics.find(lookupIt->second);

    Y_ABORT_UNLESS(tableMetricsIt != PerTableMetrics.end(),
        "The entry for the detailed metrics for the tablet ID %" PRIu64 " and the follower ID %" PRIu32 " is missing "
        "for the table ID %" PRIu64 " in the tables map", tabletId, followerId, lookupIt->second);

    TabletIdFollowerIdToTableIdMap.erase(lookupIt);

    auto partitionMetricsIt = tableMetricsIt->second.PerPartitionMetrics.find(tabletId);

    Y_ABORT_UNLESS(partitionMetricsIt != tableMetricsIt->second.PerPartitionMetrics.end(),
        "The entry for the detailed metrics for the tablet ID %" PRIu64 " and the follower ID %" PRIu32 " is missing "
        "for the table %s (table ID %" PRIu64 ") in the partitions map", tabletId, followerId,
        tableMetricsIt->second.TableMetricsConfig.TablePath.c_str(), tableMetricsIt->second.TableMetricsConfig.TableId);

    auto replicaMetricsIt = partitionMetricsIt->second.PerReplicaMetrics.find(followerId);

    Y_ABORT_UNLESS(replicaMetricsIt != partitionMetricsIt->second.PerReplicaMetrics.end(),
        "The entry for the detailed metrics for the tablet ID %" PRIu64 " and the follower ID %" PRIu32 " is missing "
        "for the table %s (table ID %" PRIu64 ") in the replicas map", tabletId, followerId,
        tableMetricsIt->second.TableMetricsConfig.TablePath.c_str(), tableMetricsIt->second.TableMetricsConfig.TableId);

    // Remove the entry for this follower ID and update the parent partition
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
        "Deleting the entry for detailed metrics for the follower ID " << followerId << ", " <<
        "tablet ID " << tabletId << " (" <<
        "type " << TTabletTypes::TypeToStr(tableMetricsIt->second.PartitionReplicaTabletType) << "), " <<
        "table " << tableMetricsIt->second.TableMetricsConfig.TablePath << " (" <<
        "table ID " << tableMetricsIt->second.TableMetricsConfig.TableId << "), " <<
        "tenant database " << TenantPathId);

    partitionMetricsIt->second.PartitionCounterGroup->RemoveSubgroup("follower_id", ToString(followerId));

    if (followerId == 0) {
        partitionMetricsIt->second.PartitionYdbMetricsAggregator->RemoveSourceCountersGroup(ToString(followerId));
    } else {
        partitionMetricsIt->second.AllFollowersYdbMetricsAggregator->RemoveSourceCountersGroup(ToString(followerId));
    }

    partitionMetricsIt->second.PerReplicaMetrics.erase(replicaMetricsIt);

    if (!partitionMetricsIt->second.PerReplicaMetrics.empty()) {
        // The parent partition still contains some replicas, just update the counters
        if (followerId != 0) {
            partitionMetricsIt->second.AllFollowersYdbMetricsAggregator->RecalculateAllTargetCounters();
        }
        partitionMetricsIt->second.PartitionYdbMetricsAggregator->RecalculateAllTargetCounters();
        tableMetricsIt->second.PerPartitionTableYdbMetricsAggregator->RecalculateAllTargetCounters();

        return;
    }

    LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
        "Deleting the entry for detailed metrics for the tablet ID " << tabletId << " (" <<
        "type " << TTabletTypes::TypeToStr(tableMetricsIt->second.PartitionReplicaTabletType) << "), " <<
        "table " << tableMetricsIt->second.TableMetricsConfig.TablePath << " (" <<
        "table ID " << tableMetricsIt->second.TableMetricsConfig.TableId << "), " <<
        "tenant database " << TenantPathId);

    tableMetricsIt->second.PerPartitionCounterGroup->RemoveSubgroup("tablet_id", ToString(tabletId));
    tableMetricsIt->second.PerPartitionTableYdbMetricsAggregator->RemoveSourceCountersGroup(ToString(tabletId));
    tableMetricsIt->second.PerPartitionMetrics.erase(partitionMetricsIt);

    if (!tableMetricsIt->second.PerPartitionMetrics.empty()) {
        // The parent table still contains some partitions, just update the counters
        tableMetricsIt->second.PerPartitionTableYdbMetricsAggregator->RecalculateAllTargetCounters();
        return;
    }

    // The parent table does not contain any partitions, delete the entry
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_AGGREGATOR,
        "Deleting the entry for detailed metrics for the table " << tableMetricsIt->second.TableMetricsConfig.TablePath << " (" <<
        "table ID " << tableMetricsIt->second.TableMetricsConfig.TableId << "), " <<
        "tenant database " << TenantPathId
    );

    if (tableMetricsIt->second.MonitoringProjectCounterGroup) {
        tableMetricsIt->second.MonitoringProjectCounterGroup->RemoveSubgroup("table", tableMetricsIt->second.TableMetricsConfig.TablePath);
    } else {
        DatabaseCounterGroup->RemoveSubgroup("table", tableMetricsIt->second.TableMetricsConfig.TablePath);
    }

    PerTableMetrics.erase(tableMetricsIt);
}

} // namespace NKikimr

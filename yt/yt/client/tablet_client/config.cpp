#include "config.h"

namespace NYT::NTabletClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TTableMountCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reject_if_entry_is_requested_but_not_ready", &TThis::RejectIfEntryIsRequestedButNotReady)
        .Default(false);

    registrar.Preprocessor([] (TThis* config) {
        config->ExpireAfterAccessTime = TDuration::Minutes(30);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(30);
        config->RefreshTime = TDuration::Seconds(30);
    });
}

TTableMountCacheConfigPtr TTableMountCacheConfig::ApplyDynamic(
    const TTableMountCacheDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    mergedConfig->ApplyDynamicInplace(dynamicConfig);
    UpdateYsonStructField(mergedConfig->RejectIfEntryIsRequestedButNotReady, dynamicConfig->RejectIfEntryIsRequestedButNotReady);
    mergedConfig->Postprocess();
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TTableMountCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reject_if_entry_is_requested_but_not_ready", &TThis::RejectIfEntryIsRequestedButNotReady)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TRemoteDynamicStoreReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("client_read_timeout", &TThis::ClientReadTimeout)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("server_read_timeout", &TThis::ServerReadTimeout)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("client_write_timeout", &TThis::ClientWriteTimeout)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("server_write_timeout", &TThis::ServerWriteTimeout)
        .Default(TDuration::Seconds(20));
    // Typical time for store flush is 15 minutes.
    registrar.Parameter("request_timeout", &TThis::RequestTimeout)
        .Default(TDuration::Minutes(20));
    registrar.Parameter("max_rows_per_server_read", &TThis::MaxRowsPerServerRead)
        .GreaterThan(0)
        .Default(1024);

    registrar.Parameter("window_size", &TThis::WindowSize)
        .Default(16_MB)
        .GreaterThan(0);

    registrar.Parameter("streaming_subrequest_failure_probability", &TThis::StreamingSubrequestFailureProbability)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

void TRetryingRemoteDynamicStoreReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("retry_count", &TThis::RetryCount)
        .Default(10)
        .GreaterThan(0);
    registrar.Parameter("locate_request_backoff_time", &TThis::LocateRequestBackoffTime)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TReplicationCollocationOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("preferred_sync_replica_clusters", &TThis::PreferredSyncReplicaClusters)
        .Default(std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

void TReplicatedTableOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("max_sync_replica_count", &TThis::MaxSyncReplicaCount)
        .Alias("sync_replica_count")
        .Optional()
        .GreaterThanOrEqual(0);
    registrar.Parameter("min_sync_replica_count", &TThis::MinSyncReplicaCount)
        .Optional()
        .GreaterThanOrEqual(0);
    registrar.Parameter("max_sync_queue_replica_count", &TThis::MaxSyncQueueReplicaCount)
        .Optional()
        .GreaterThanOrEqual(2)
        .DontSerializeDefault();
    registrar.Parameter("min_sync_queue_replica_count", &TThis::MinSyncQueueReplicaCount)
        .Optional()
        .GreaterThanOrEqual(1)
        .DontSerializeDefault();

    registrar.Parameter("enable_replicated_table_tracker", &TThis::EnableReplicatedTableTracker)
        .Default(false);

    registrar.Parameter("sync_replica_lag_threshold", &TThis::SyncReplicaLagThreshold)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("tablet_cell_bundle_name_ttl", &TThis::TabletCellBundleNameTtl)
        .Default(TDuration::Seconds(300));
    registrar.Parameter("tablet_cell_bundle_name_failure_interval", &TThis::RetryOnFailureInterval)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("enable_preload_state_check", &TThis::EnablePreloadStateCheck)
        .Default(false);
    registrar.Parameter("incomplete_preload_grace_period", &TThis::IncompletePreloadGracePeriod)
        .Default(TDuration::Minutes(5));

    registrar.Postprocessor([] (TThis* config) {
        if (config->MaxSyncReplicaCount &&
            config->MinSyncReplicaCount &&
            config->MinSyncReplicaCount > config->MaxSyncReplicaCount)
        {
            THROW_ERROR_EXCEPTION("\"min_sync_replica_count\" must be less or equal to \"max_sync_replica_count\"");
        }

        if (config->MaxSyncQueueReplicaCount &&
            config->MinSyncQueueReplicaCount &&
            config->MinSyncQueueReplicaCount > config->MaxSyncQueueReplicaCount)
        {
            THROW_ERROR_EXCEPTION("\"min_sync_queue_replica_count\" must be less or equal to \"max_sync_queue_replica_count\"");
        }
    });
}

std::tuple<int, int> TReplicatedTableOptions::GetEffectiveMinMaxReplicaCount(
    ETableReplicaContentType contentType,
    int replicaCount) const
{
    auto getResult = [&] (auto minSyncReplicaCount, auto maxSyncReplicaCount) {
        int maxSyncReplicas = 0;
        int minSyncReplicas = 0;

        if (!maxSyncReplicaCount && !minSyncReplicaCount) {
            maxSyncReplicas = 1;
        } else {
            maxSyncReplicas = maxSyncReplicaCount.value_or(replicaCount);
        }

        minSyncReplicas = minSyncReplicaCount.value_or(maxSyncReplicas);

        return std::tuple(minSyncReplicas, maxSyncReplicas);
    };

    if (contentType == ETableReplicaContentType::Queue) {
        int minSyncReplicas;
        int maxSyncReplicas;
        if (MinSyncQueueReplicaCount || MaxSyncQueueReplicaCount) {
            std::tie(minSyncReplicas, maxSyncReplicas) = getResult(MinSyncQueueReplicaCount, MaxSyncQueueReplicaCount);
        } else {
            std::tie(minSyncReplicas, maxSyncReplicas) = getResult(MinSyncReplicaCount, MaxSyncReplicaCount);
        }
        return std::tuple(
            std::max(minSyncReplicas, 1),
            std::max(maxSyncReplicas, 2));
    } else {
        return getResult(MinSyncReplicaCount, MaxSyncReplicaCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

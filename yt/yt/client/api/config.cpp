#include "config.h"

#include <yt/yt/core/misc/config.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

void TTableMountCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("on_error_retry_count", &TThis::OnErrorRetryCount)
        .GreaterThanOrEqual(0)
        .Default(5);
    registrar.Parameter("on_error_retry_slack_period", &TThis::OnErrorSlackPeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("connection_type", &TThis::ConnectionType)
        .Default(EConnectionType::Native);
    registrar.Parameter("cluster_name", &TThis::ClusterName)
        .Default();
    registrar.Parameter("table_mount_cache", &TThis::TableMountCache)
        .DefaultNew();
    registrar.Parameter("replication_card_cache", &TThis::ReplicationCardCache)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TConnectionDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("table_mount_cache", &TThis::TableMountCache)
        .DefaultNew();
    registrar.Parameter("tablet_write_backoff", &TThis::TabletWriteBackoff)
        .Default({
            .InvocationCount = 0,
        });
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentQueuePollerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_prefetch_row_count", &TThis::MaxPrefetchRowCount)
        .GreaterThan(0)
        .Default(1024);
    registrar.Parameter("max_prefetch_data_weight", &TThis::MaxPrefetchDataWeight)
        .GreaterThan(0)
        .Default((i64) 16 * 1024 * 1024);
    registrar.Parameter("max_rows_per_fetch", &TThis::MaxRowsPerFetch)
        .GreaterThan(0)
        .Default(512);
    registrar.Parameter("max_rows_per_poll", &TThis::MaxRowsPerPoll)
        .GreaterThan(0)
        .Default(1);
    registrar.Parameter("max_fetched_untrimmed_row_count", &TThis::MaxFetchedUntrimmedRowCount)
        .GreaterThan(0)
        .Default(40000);
    registrar.Parameter("untrimmed_data_rows_low", &TThis::UntrimmedDataRowsLow)
        .Default(0);
    registrar.Parameter("untrimmed_data_rows_high", &TThis::UntrimmedDataRowsHigh)
        .Default(std::numeric_limits<i64>::max());
    registrar.Parameter("data_poll_period", &TThis::DataPollPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("state_trim_period", &TThis::StateTrimPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("backoff_time", &TThis::BackoffTime)
        .Default(TDuration::Seconds(5));

    registrar.Postprocessor([] (TThis* config) {
        if (config->UntrimmedDataRowsLow > config->UntrimmedDataRowsHigh) {
            THROW_ERROR_EXCEPTION("\"untrimmed_data_rows_low\" must not exceed \"untrimmed_data_rows_high\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJournalChunkWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_batch_row_count", &TThis::MaxBatchRowCount)
        .Default(256);
    registrar.Parameter("max_batch_data_size", &TThis::MaxBatchDataSize)
        .Default(16_MB);
    registrar.Parameter("max_batch_delay", &TThis::MaxBatchDelay)
        .Default(TDuration::MilliSeconds(5));

    registrar.Parameter("max_flush_row_count", &TThis::MaxFlushRowCount)
        .Default(100'000);
    registrar.Parameter("max_flush_data_size", &TThis::MaxFlushDataSize)
        .Default(100_MB);

    registrar.Parameter("prefer_local_host", &TThis::PreferLocalHost)
        .Default(true);

    registrar.Parameter("node_rpc_timeout", &TThis::NodeRpcTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("node_ping_period", &TThis::NodePingPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("node_ban_timeout", &TThis::NodeBanTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("node_channel", &TThis::NodeChannel)
        .DefaultNew();

    registrar.Parameter("replica_failure_probability", &TThis::ReplicaFailureProbability)
        .Default(0.0)
        .InRange(0.0, 1.0);
    registrar.Parameter("replica_row_limits", &TThis::ReplicaRowLimits)
        .Default();
    registrar.Parameter("replica_fake_timeout_delay", &TThis::ReplicaFakeTimeoutDelay)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->MaxBatchRowCount > config->MaxFlushRowCount) {
            THROW_ERROR_EXCEPTION("\"max_batch_row_count\" cannot be greater than \"max_flush_row_count\"")
                << TErrorAttribute("max_batch_row_count", config->MaxBatchRowCount)
                << TErrorAttribute("max_flush_row_count", config->MaxFlushRowCount);
        }
        if (config->MaxBatchDataSize > config->MaxFlushDataSize) {
            THROW_ERROR_EXCEPTION("\"max_batch_data_size\" cannot be greater than \"max_flush_data_size\"")
                << TErrorAttribute("max_batch_data_size", config->MaxBatchDataSize)
                << TErrorAttribute("max_flush_data_size", config->MaxFlushDataSize);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJournalWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_chunk_row_count", &TThis::MaxChunkRowCount)
        .GreaterThan(0)
        .Default(1'000'000);
    registrar.Parameter("max_chunk_data_size", &TThis::MaxChunkDataSize)
        .GreaterThan(0)
        .Default(10_GB);
    registrar.Parameter("max_chunk_session_duration", &TThis::MaxChunkSessionDuration)
        .Default(TDuration::Hours(60));

    registrar.Parameter("open_session_backoff_time", &TThis::OpenSessionBackoffTime)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("open_session_retry_count", &TThis::OpenSessionRetryCount)
        .Default(5);

    registrar.Parameter("prerequisite_transaction_probe_period", &TThis::PrerequisiteTransactionProbePeriod)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("dont_close", &TThis::DontClose)
        .Default(false);
    registrar.Parameter("dont_seal", &TThis::DontSeal)
        .Default(false);
    registrar.Parameter("dont_preallocate", &TThis::DontPreallocate)
        .Default(false);
    registrar.Parameter("open_delay", &TThis::OpenDelay)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TJournalChunkWriterOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("replication_factor", &TThis::ReplicationFactor)
        .Default(3);
    registrar.Parameter("erasure_codec", &TThis::ErasureCodec)
        .Default(NErasure::ECodec::None);

    registrar.Parameter("read_quorum", &TThis::ReadQuorum)
        .Default(2);
    registrar.Parameter("write_quorum", &TThis::WriteQuorum)
        .Default(2);

    registrar.Parameter("replica_lag_limit", &TThis::ReplicaLagLimit)
        .Default(NJournalClient::DefaultReplicaLagLimit);

    registrar.Parameter("enable_multiplexing", &TThis::EnableMultiplexing)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi


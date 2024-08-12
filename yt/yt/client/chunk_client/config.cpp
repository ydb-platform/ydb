#include "config.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void TFetchChunkSpecConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_chunks_per_fetch", &TThis::MaxChunksPerFetch)
        .GreaterThan(0)
        .Default(100000);
    registrar.Parameter("max_chunks_per_locate_request", &TThis::MaxChunksPerLocateRequest)
        .GreaterThan(0)
        .Default(10000);
}

////////////////////////////////////////////////////////////////////////////////

void TFetcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("node_rpc_timeout", &TThis::NodeRpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("node_ban_duration", &TThis::NodeBanDuration)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("backoff_time", &TThis::BackoffTime)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("max_chunks_per_node_fetch", &TThis::MaxChunksPerNodeFetch)
        .Default(500);

    registrar.Parameter("node_directory_synchronization_timeout", &TThis::NodeDirectorySynchronizationTimeout)
        .Default(TDuration::Minutes(5));
}

////////////////////////////////////////////////////////////////////////////////

void TBlockReordererConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_block_reordering", &TThis::EnableBlockReordering)
        .Default(false);

    registrar.Parameter("shuffle_blocks", &TThis::ShuffleBlocks)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkSliceFetcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_slices_per_fetch", &TThis::MaxSlicesPerFetch)
        .GreaterThan(0)
        .Default(10'000);
}

////////////////////////////////////////////////////////////////////////////////

void TEncodingWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("encode_window_size", &TThis::EncodeWindowSize)
        .Default(16_MB)
        .GreaterThan(0);
    registrar.Parameter("default_compression_ratio", &TThis::DefaultCompressionRatio)
        .Default(0.2);
    registrar.Parameter("verify_compression", &TThis::VerifyCompression)
        .Default(true);
    registrar.Parameter("compute_checksum", &TThis::ComputeChecksum)
        .Default(true);
    registrar.Parameter("compression_concurrency", &TThis::CompressionConcurrency)
        .Default(1)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

void TRemoteReaderConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("disk_queue_size_factor", &TThis::DiskQueueSizeFactor)
        .Default(1.0);
    registrar.Parameter("net_queue_size_factor", &TThis::NetQueueSizeFactor)
        .Default(0.5);

    registrar.Parameter("suspicious_node_grace_period", &TThis::SuspiciousNodeGracePeriod)
        .Default();

    registrar.Parameter("use_direct_io", &TThis::UseDirectIO)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TReplicationReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("block_rpc_timeout", &TThis::BlockRpcTimeout)
        .Default(TDuration::Seconds(120));
    registrar.Parameter("block_rpc_hedging_delay", &TThis::BlockRpcHedgingDelay)
        .Default();
    registrar.Parameter("lookup_rpc_hedging_delay", &TThis::LookupRpcHedgingDelay)
        .Default();
    registrar.Parameter("cancel_primary_block_rpc_request_on_hedging", &TThis::CancelPrimaryBlockRpcRequestOnHedging)
        .Default(false);
    registrar.Parameter("cancel_primary_lookup_rpc_request_on_hedging", &TThis::CancelPrimaryLookupRpcRequestOnHedging)
        .Default(false);
    registrar.Parameter("lookup_rpc_timeout", &TThis::LookupRpcTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("meta_rpc_timeout", &TThis::MetaRpcTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("meta_rpc_hedging_delay", &TThis::MetaRpcHedgingDelay)
        .Default();
    registrar.Parameter("probe_rpc_timeout", &TThis::ProbeRpcTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("probe_peer_count", &TThis::ProbePeerCount)
        .Default(3)
        .GreaterThan(0);
    registrar.Parameter("retry_count", &TThis::RetryCount)
        .Default(20);
    registrar.Parameter("fail_on_no_seeds", &TThis::FailOnNoSeeds)
        .Default(false);
    registrar.Parameter("min_backoff_time", &TThis::MinBackoffTime)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("max_backoff_time", &TThis::MaxBackoffTime)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("backoff_time_multiplier", &TThis::BackoffTimeMultiplier)
        .GreaterThan(1)
        .Default(1.5);
    registrar.Parameter("pass_count", &TThis::PassCount)
        .Default(500);
    registrar.Parameter("fetch_from_peers", &TThis::FetchFromPeers)
        .Default(true);
    registrar.Parameter("peer_expiration_timeout", &TThis::PeerExpirationTimeout)
        .Default(TDuration::Seconds(300));
    registrar.Parameter("populate_cache", &TThis::PopulateCache)
        .Default(true);
    registrar.Parameter("prefer_local_host", &TThis::PreferLocalHost)
        .Default(false);
    registrar.Parameter("prefer_local_rack", &TThis::PreferLocalRack)
        .Default(false);
    registrar.Parameter("prefer_local_data_center", &TThis::PreferLocalDataCenter)
        .Default(true);
    registrar.Parameter("max_ban_count", &TThis::MaxBanCount)
        .Default(5);
    registrar.Parameter("enable_workload_fifo_scheduling", &TThis::EnableWorkloadFifoScheduling)
        .Default(true);
    registrar.Parameter("retry_timeout", &TThis::RetryTimeout)
        .Default(TDuration::Minutes(3));
    registrar.Parameter("session_timeout", &TThis::SessionTimeout)
        .Default(TDuration::Minutes(20));
    registrar.Parameter("lookup_request_pass_count", &TThis::LookupRequestPassCount)
        .GreaterThan(0)
        .Default(10);
    registrar.Parameter("lookup_request_retry_count", &TThis::LookupRequestRetryCount)
        .GreaterThan(0)
        .Default(5);
    registrar.Parameter("use_async_block_cache", &TThis::UseAsyncBlockCache)
        .Default(false);
    registrar.Parameter("use_block_cache", &TThis::UseBlockCache)
        .Default(true);
    registrar.Parameter("prolonged_discard_seeds_delay", &TThis::ProlongedDiscardSeedsDelay)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("enable_chunk_meta_cache", &TThis::EnableChunkMetaCache)
        .Default(true);
    registrar.Parameter("ban_peers_permanently", &TThis::BanPeersPermanently)
        .Default(true);
    registrar.Parameter("enable_local_throttling", &TThis::EnableLocalThrottling)
        .Default(false);
    registrar.Parameter("chunk_meta_cache_failure_probability", &TThis::ChunkMetaCacheFailureProbability)
        .Default();
    registrar.Parameter("use_chunk_prober", &TThis::UseChunkProber)
        .Default(false);
    registrar.Parameter("use_read_blocks_batcher", &TThis::UseReadBlocksBatcher)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        // Seems unreasonable to make backoff greater than half of total session timeout.
        config->MaxBackoffTime = std::min(config->MaxBackoffTime, config->SessionTimeout / 2);
        config->RetryTimeout = std::min(config->RetryTimeout, config->SessionTimeout);

        // Rpc timeout should not exceed session timeout.
        config->BlockRpcTimeout = std::min(config->BlockRpcTimeout, config->RetryTimeout);
        config->LookupRpcTimeout = std::min(config->LookupRpcTimeout, config->RetryTimeout);
        config->MetaRpcTimeout = std::min(config->MetaRpcTimeout, config->RetryTimeout);
        config->ProbeRpcTimeout = std::min(config->ProbeRpcTimeout, config->RetryTimeout);

        // These are supposed to be not greater than PassCount and RetryCount.
        config->LookupRequestPassCount = std::min(config->LookupRequestPassCount, config->PassCount);
        config->LookupRequestRetryCount = std::min(config->LookupRequestRetryCount, config->RetryCount);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TBlockFetcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("window_size", &TThis::WindowSize)
        .Default(20_MB)
        .GreaterThan(0);
    registrar.Parameter("group_size", &TThis::GroupSize)
        .Default(15_MB)
        .GreaterThan(0);

    registrar.Parameter("use_uncompressed_block_cache", &TThis::UseUncompressedBlockCache)
        .Default(true);

    registrar.Postprocessor([] (TThis* config) {
        if (config->GroupSize > config->WindowSize) {
            THROW_ERROR_EXCEPTION("\"group_size\" cannot be larger than \"window_size\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TErasureReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_auto_repair", &TThis::EnableAutoRepair)
        .Default(true);
    registrar.Parameter("replication_reader_speed_limit_per_sec", &TThis::ReplicationReaderSpeedLimitPerSec)
        .Default(5_MB);
    registrar.Parameter("slow_reader_expiration_timeout", &TThis::SlowReaderExpirationTimeout)
        .Default(TDuration::Minutes(2));
    registrar.Parameter("replication_reader_timeout", &TThis::ReplicationReaderTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("replication_reader_failure_timeout", &TThis::ReplicationReaderFailureTimeout)
        .Default(TDuration::Minutes(10));
}

////////////////////////////////////////////////////////////////////////////////

void TMultiChunkReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_buffer_size", &TThis::MaxBufferSize)
        .GreaterThan(0L)
        .LessThanOrEqual(10_GB)
        .Default(100_MB);
    registrar.Parameter("max_parallel_readers", &TThis::MaxParallelReaders)
        .GreaterThanOrEqual(1)
        .LessThanOrEqual(1000)
        .Default(512);

    registrar.Postprocessor([] (TThis* config) {
        if (config->MaxBufferSize < 2 * config->WindowSize) {
            THROW_ERROR_EXCEPTION("\"max_buffer_size\" cannot be less than twice \"window_size\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TReplicationWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("send_window_size", &TThis::SendWindowSize)
        .Default(32_MB)
        .GreaterThan(0);
    registrar.Parameter("group_size", &TThis::GroupSize)
        .Default(10_MB)
        .GreaterThan(0);
    registrar.Parameter("node_channel", &TThis::NodeChannel)
        .DefaultNew();
    registrar.Parameter("node_rpc_timeout", &TThis::NodeRpcTimeout)
        .Default(TDuration::Seconds(300));
    registrar.Parameter("upload_replication_factor", &TThis::UploadReplicationFactor)
        .GreaterThanOrEqual(1)
        .Default(2);
    registrar.Parameter("ban_failed_nodes", &TThis::BanFailedNodes)
        .Default(true);
    registrar.Parameter("min_upload_replication_factor", &TThis::MinUploadReplicationFactor)
        .Default(2)
        .GreaterThanOrEqual(1);
    registrar.Parameter("direct_upload_node_count", &TThis::DirectUploadNodeCount)
        .Default();
    registrar.Parameter("prefer_local_host", &TThis::PreferLocalHost)
        .Default(true);
    registrar.Parameter("node_ping_interval", &TThis::NodePingPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("populate_cache", &TThis::PopulateCache)
        .Default(false);
    registrar.Parameter("sync_on_close", &TThis::SyncOnClose)
        .Default(true);
    registrar.Parameter("enable_direct_io", &TThis::EnableDirectIO)
        .Default(false);
    registrar.Parameter("enable_early_finish", &TThis::EnableEarlyFinish)
        .Default(false);
    registrar.Parameter("allocate_write_targets_backoff_time", &TThis::AllocateWriteTargetsBackoffTime)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("allocate_write_targets_retry_count", &TThis::AllocateWriteTargetsRetryCount)
        .Default(10);

    registrar.Parameter("testing_delay", &TThis::TestingDelay)
        .Default();

    registrar.Parameter("enable_local_throttling", &TThis::EnableLocalThrottling)
        .Default(false);

    registrar.Preprocessor([] (TThis* config) {
        config->NodeChannel->RetryBackoffTime = TDuration::Seconds(10);
        config->NodeChannel->RetryAttempts = 100;
    });

    registrar.Postprocessor([] (TThis* config) {
        if (!config->DirectUploadNodeCount) {
            return;
        }

        if (*config->DirectUploadNodeCount < 1) {
            THROW_ERROR_EXCEPTION("\"direct_upload_node_count\" cannot be less that 1");
        }
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->SendWindowSize < config->GroupSize) {
            THROW_ERROR_EXCEPTION("\"send_window_size\" cannot be less than \"group_size\"");
        }
    });
}

int TReplicationWriterConfig::GetDirectUploadNodeCount()
{
    auto replicationFactor = std::min(MinUploadReplicationFactor, UploadReplicationFactor);
    if (DirectUploadNodeCount) {
        return std::min(*DirectUploadNodeCount, replicationFactor);
    }

    return std::max(static_cast<int>(std::sqrt(replicationFactor)), 1);
}

////////////////////////////////////////////////////////////////////////////////

void TErasureWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_erasure_target_node_reallocation", &TThis::EnableErasureTargetNodeReallocation)
        .Default(false);

    registrar.Parameter("erasure_window_size", &TThis::ErasureWindowSize)
        .Default(8_MB)
        .GreaterThan(0);

    registrar.Parameter("writer_window_size", &TThis::WriterWindowSize)
        .Default(64_MB)
        .GreaterThan(0);
    registrar.Parameter("writer_group_size", &TThis::WriterGroupSize)
        .Default(16_MB)
        .GreaterThan(0);

    registrar.Parameter("desired_segment_part_size", &TThis::DesiredSegmentPartSize)
        .Default(0)
        .GreaterThanOrEqual(0);

    registrar.Parameter("erasure_store_original_block_checksums", &TThis::ErasureStoreOriginalBlockChecksums)
        .Default(false);

    registrar.Parameter("enable_striped_erasure", &TThis::EnableStripedErasure)
        .Default(false);

    registrar.Parameter("erasure_stripe_size", &TThis::ErasureStripeSize)
        .Default()
        .GreaterThan(0);

    registrar.Parameter("use_effective_erasure_codecs", &TThis::UseEffectiveErasureCodecs)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TMultiChunkWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("desired_chunk_size", &TThis::DesiredChunkSize)
        .GreaterThan(0)
        .Default(2_GB);

    registrar.Parameter("desired_chunk_weight", &TThis::DesiredChunkWeight)
        .GreaterThan(0)
        .Default(100_GB);

    registrar.Parameter("max_meta_size", &TThis::MaxMetaSize)
        .GreaterThan(0)
        .LessThanOrEqual(64_MB)
        .Default(30_MB);
}

////////////////////////////////////////////////////////////////////////////////

void TEncodingWriterOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("compression_codec", &TThis::CompressionCodec)
        .Default(NCompression::ECodec::None);
    registrar.Parameter("chunks_eden", &TThis::ChunksEden)
        .Default(false);
    registrar.Parameter("set_chunk_creation_time", &TThis::SetChunkCreationTime)
        .Default(true)
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

void TChunkFragmentReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("peer_info_expiration_timeout", &TThis::PeerInfoExpirationTimeout)
        .Default(TDuration::Minutes(30));

    registrar.Parameter("seeds_expiration_timeout", &TThis::SeedsExpirationTimeout)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("periodic_update_delay", &TThis::PeriodicUpdateDelay)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(10));

    registrar.Parameter("probe_chunk_set_rpc_timeout", &TThis::ProbeChunkSetRpcTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("get_chunk_fragment_set_rpc_timeout", &TThis::GetChunkFragmentSetRpcTimeout)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("fragment_read_hedging_delay", &TThis::FragmentReadHedgingDelay)
        .Default();

    registrar.Parameter("retry_count_limit", &TThis::RetryCountLimit)
        .GreaterThanOrEqual(1)
        .Default(10);
    registrar.Parameter("retry_backoff_time", &TThis::RetryBackoffTime)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("read_time_limit", &TThis::ReadTimeLimit)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("chunk_info_cache_expiration_timeout", &TThis::ChunkInfoCacheExpirationTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("max_inflight_fragment_length", &TThis::MaxInflightFragmentLength)
        .Default(16_MB);
    registrar.Parameter("max_inflight_fragment_count", &TThis::MaxInflightFragmentCount)
        .Default(8192);

    registrar.Parameter("prefetch_whole_blocks", &TThis::PrefetchWholeBlocks)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

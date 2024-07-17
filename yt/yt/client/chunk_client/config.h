#pragma once

#include "public.h"

#include <yt/yt/client/misc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TFetchChunkSpecConfig
    : public virtual NYTree::TYsonStruct
{
public:
    int MaxChunksPerFetch;
    int MaxChunksPerLocateRequest;

    REGISTER_YSON_STRUCT(TFetchChunkSpecConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFetchChunkSpecConfig)

////////////////////////////////////////////////////////////////////////////////

class TFetcherConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TDuration NodeRpcTimeout;

    //! If node throttled fetch request, it becomes banned for this period of time.
    TDuration NodeBanDuration;

    //! Time to sleep before next fetching round if no requests were performed.
    TDuration BackoffTime;

    int MaxChunksPerNodeFetch;

    //! Timeout when waiting for all replicas to appear in the given node directory.
    TDuration NodeDirectorySynchronizationTimeout;

    REGISTER_YSON_STRUCT(TFetcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFetcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlockReordererConfig
    : public virtual NYTree::TYsonStruct
{
public:
    bool EnableBlockReordering;

    //! Instead of grouping blocks by column groups, shuffle them.
    //! Used only for testing purposes.
    bool ShuffleBlocks;

    REGISTER_YSON_STRUCT(TBlockReordererConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlockReordererConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkSliceFetcherConfig
    : public TFetcherConfig
{
public:
    int MaxSlicesPerFetch;

    REGISTER_YSON_STRUCT(TChunkSliceFetcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkSliceFetcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TEncodingWriterConfig
    : public virtual TWorkloadConfig
    , public virtual TBlockReordererConfig
{
public:
    i64 EncodeWindowSize;
    double DefaultCompressionRatio;
    bool VerifyCompression;
    bool ComputeChecksum;
    int CompressionConcurrency;

    REGISTER_YSON_STRUCT(TEncodingWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TEncodingWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteReaderConfigBase
    : public virtual NYTree::TYsonStruct
{
public:
    //! Factors to calculate peer load as linear combination of disk queue and net queue.
    double NetQueueSizeFactor;
    double DiskQueueSizeFactor;

    //! Will locate new replicas from master
    //! if node was suspicious for at least the period (unless null).
    std::optional<TDuration> SuspiciousNodeGracePeriod;

    //! Will open and read with DirectIO (unless already opened w/o DirectIO or disabled via location config).
    bool UseDirectIO;

    REGISTER_YSON_STRUCT(TRemoteReaderConfigBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationReaderConfig
    : public virtual TRemoteReaderConfigBase
{
public:
    //! Timeout for a block request.
    TDuration BlockRpcTimeout;

    //! Delay before sending a hedged block request. If null then hedging is disabled.
    //! NB: Hedging policy may be overridden via hedging manager.
    std::optional<TDuration> BlockRpcHedgingDelay;

    //! Same as above but for a LookupRows rpc.
    std::optional<TDuration> LookupRpcHedgingDelay;

    //! Whether to cancel the primary block request when backup one is sent.
    bool CancelPrimaryBlockRpcRequestOnHedging;

    //! Same as above but for a LookupRows rpc.
    bool CancelPrimaryLookupRpcRequestOnHedging;

    //! Timeout for a lookup request.
    TDuration LookupRpcTimeout;

    //! Timeout for a meta request.
    TDuration MetaRpcTimeout;

    //! Delay before sending for a hedged meta request. If null then hedging is disabled.
    //! NB: Hedging policy may be overridden via hedging manager.
    std::optional<TDuration> MetaRpcHedgingDelay;

    //! Timeout for a queue size probing request.
    TDuration ProbeRpcTimeout;

    //! Maximum number of peers to poll for queue length each round.
    int ProbePeerCount;

    //! Maximum number of attempts to fetch new seeds.
    int RetryCount;

    //! Fail read session immediately if master reports no seeds for chunk.
    bool FailOnNoSeeds;

    //! Time to wait before making another pass with same seeds.
    //! Increases exponentially with every pass, from MinPassBackoffTime to MaxPassBackoffTime.
    TDuration MinBackoffTime;
    TDuration MaxBackoffTime;
    double BackoffTimeMultiplier;

    //! Maximum number of passes with same seeds.
    int PassCount;

    //! Enable fetching blocks from peers suggested by seeds.
    bool FetchFromPeers;

    //! Timeout after which a node forgets about the peer.
    //! Only makes sense if the reader is equipped with peer descriptor.
    TDuration PeerExpirationTimeout;

    //! If |true| then fetched blocks are cached by the node.
    bool PopulateCache;

    //! If |true| then local data center replicas are unconditionally preferred to remote replicas.
    bool PreferLocalDataCenter;

    //! If |true| then local rack replicas are unconditionally preferred to remote replicas.
    bool PreferLocalRack;

    //! If |true| then local host replicas are unconditionally preferred to any other replicas.
    bool PreferLocalHost;

    //! If peer ban counter exceeds #MaxBanCount, peer is banned forever.
    int MaxBanCount;

    //! If |true|, then workload descriptors are annotated with the read session start time
    //! and are thus scheduled in FIFO order.
    bool EnableWorkloadFifoScheduling;

    //! Total retry timeout, helps when we are doing too many passes.
    TDuration RetryTimeout;

    //! Total session timeout (for ReadBlocks and GetMeta calls).
    TDuration SessionTimeout;

    //! Maximum number of passes within single retry for lookup request.
    int LookupRequestPassCount;

    //! Maximum number of retries for lookup request.
    int LookupRequestRetryCount;

    //! If |true| block cache will be accessed via asynchronous interface, if |false|
    //! synchronous interface will be used.
    bool UseAsyncBlockCache;

    //! If |true| replication reader will try to fetch blocks from local block cache.
    bool UseBlockCache;

    //! Is used to increase interval between Locates
    //! that are called for discarding seeds that are suspicious.
    TDuration ProlongedDiscardSeedsDelay;

    //! If |true| GetMeta() will be performed via provided ChunkMetaCache.
    //! If ChunkMetaCache is nullptr or partition tag is specified, this option has no effect.
    bool EnableChunkMetaCache;

    //! If |true| reader will retain a set of peers that will be banned for every session.
    bool BanPeersPermanently;

    //! If |true| network throttlers will be applied even in case of requests to local host.
    bool EnableLocalThrottling;

    //! For testing purposes.
    //! Unless null, reader will simulate failure of accessing chunk meta cache with such probability.
    std::optional<double> ChunkMetaCacheFailureProbability;

    //! Use chunk prober to reduce the number of probing requests.
    bool UseChunkProber;

    //! Use request batcher to reduce the number of get blocks requests.
    bool UseReadBlocksBatcher;

    REGISTER_YSON_STRUCT(TReplicationReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlockFetcherConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Prefetch window size (in bytes).
    i64 WindowSize;

    //! Maximum amount of data to be transferred via a single RPC request.
    i64 GroupSize;

    //! If |True| block fetcher will try to fetch block from local uncompressed block cache.
    bool UseUncompressedBlockCache;

    REGISTER_YSON_STRUCT(TBlockFetcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlockFetcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TErasureReaderConfig
    : public virtual TReplicationReaderConfig
    , public virtual TBlockFetcherConfig
{
public:
    bool EnableAutoRepair;
    double ReplicationReaderSpeedLimitPerSec;
    TDuration SlowReaderExpirationTimeout;
    TDuration ReplicationReaderTimeout;
    TDuration ReplicationReaderFailureTimeout;

    REGISTER_YSON_STRUCT(TErasureReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TErasureReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderConfig
    : public virtual TErasureReaderConfig
    , public virtual TBlockFetcherConfig
    , public virtual TFetchChunkSpecConfig
    , public virtual TWorkloadConfig
{
public:
    i64 MaxBufferSize;
    int MaxParallelReaders;

    REGISTER_YSON_STRUCT(TMultiChunkReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicationWriterConfig
    : public virtual TWorkloadConfig
    , public virtual TBlockReordererConfig
{
public:
    //! Maximum window size (in bytes).
    i64 SendWindowSize;

    //! Maximum group size (in bytes).
    i64 GroupSize;

    //! RPC requests timeout.
    /*!
     *  This timeout is especially useful for |PutBlocks| calls to ensure that
     *  uploading is not stalled.
     */
    TDuration NodeRpcTimeout;

    NRpc::TRetryingChannelConfigPtr NodeChannel;

    int UploadReplicationFactor;

    int MinUploadReplicationFactor;

    std::optional<int> DirectUploadNodeCount;

    bool PreferLocalHost;

    bool BanFailedNodes;

    //! Interval between consecutive pings to Data Nodes.
    TDuration NodePingPeriod;

    //! If |true| then written blocks are cached by the node.
    bool PopulateCache;

    //! If |true| then the chunk is fsynced to disk upon closing.
    bool SyncOnClose;

    bool EnableDirectIO;

    //! If |true| then the chunk is finished as soon as MinUploadReplicationFactor chunks are written.
    bool EnableEarlyFinish;

    TDuration AllocateWriteTargetsBackoffTime;

    int AllocateWriteTargetsRetryCount;

    std::optional<TDuration> TestingDelay;

    //! If |true| network throttlers will be applied even in case of requests to local host.
    bool EnableLocalThrottling;

    int GetDirectUploadNodeCount();

    REGISTER_YSON_STRUCT(TReplicationWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TErasureWriterConfig
    : public virtual TBlockReordererConfig
{
public:
    i64 WriterWindowSize;
    i64 WriterGroupSize;

    // TODO(gritukan): Drop.
    std::optional<i64> ErasureStripeSize;
    i64 ErasureWindowSize;
    bool ErasureStoreOriginalBlockChecksums;

    i64 DesiredSegmentPartSize;

    bool EnableErasureTargetNodeReallocation;

    bool EnableStripedErasure;

    bool UseEffectiveErasureCodecs;

    REGISTER_YSON_STRUCT(TErasureWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TErasureWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkWriterConfig
    : public TReplicationWriterConfig
    , public TErasureWriterConfig
{
public:
    i64 DesiredChunkSize;
    i64 DesiredChunkWeight;
    i64 MaxMetaSize;

    REGISTER_YSON_STRUCT(TMultiChunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TMemoryTrackedWriterOptions
    : public NYTree::TYsonStruct
{
public:
    IMemoryUsageTrackerPtr MemoryUsageTracker;
};

////////////////////////////////////////////////////////////////////////////////

class TEncodingWriterOptions
    : public virtual TMemoryTrackedWriterOptions
{
public:
    NCompression::ECodec CompressionCodec;
    bool ChunksEden;
    bool SetChunkCreationTime;

    REGISTER_YSON_STRUCT(TEncodingWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TEncodingWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TChunkFragmentReaderConfig
    : public virtual TRemoteReaderConfigBase
{
public:
    //! Expiration timeout of corresponding sync expiring cache.
    TDuration PeerInfoExpirationTimeout;

    //! Minimal delay between sequential chunk replica locations.
    TDuration SeedsExpirationTimeout;

    //! Delay between background cache updates.
    TDuration PeriodicUpdateDelay;

    //! RPC timeouts of ProbeChunkSet and GetChunkFragmentSet.
    TDuration ProbeChunkSetRpcTimeout;
    TDuration GetChunkFragmentSetRpcTimeout;

    //! Delay before sending a hedged request. If null then hedging is disabled.
    //! NB: This option may be overridden via hedging manager.
    std::optional<TDuration> FragmentReadHedgingDelay;

    //! Limit on retry count.
    int RetryCountLimit;
    //! Time between retries.
    TDuration RetryBackoffTime;
    //! Maximum time to serve fragments read request.
    TDuration ReadTimeLimit;

    //! Chunk that was not accessed for the time by user
    //! will stop being accessed within periodic updates and then will be evicted via expiring cache logic.
    TDuration ChunkInfoCacheExpirationTimeout;

    //! Upper bound on length of simultaneously requested fragments within a reading session.
    i64 MaxInflightFragmentLength;

    //! Upper bound on count of simultaneously requested fragments within a reading session.
    i64 MaxInflightFragmentCount;

    // If |true| will request full blocks and store them in a cache for further access.
    bool PrefetchWholeBlocks;

    REGISTER_YSON_STRUCT(TChunkFragmentReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkFragmentReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

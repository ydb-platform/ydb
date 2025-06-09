#pragma once

#include "public.h"

#include <yt/yt/client/journal_client/config.h>

#include <yt/yt/client/tablet_client/config.h>

#include <yt/yt/client/chaos_client/config.h>

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/file_client/config.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/backoff_strategy.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EConnectionType,
    (Native)
    (Rpc)
);

////////////////////////////////////////////////////////////////////////////////

struct TTableMountCacheConfig
    : public NTabletClient::TTableMountCacheConfig
{
    int OnErrorRetryCount;
    TDuration OnErrorSlackPeriod;

    REGISTER_YSON_STRUCT(TTableMountCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TConnectionConfig
    : public virtual NYTree::TYsonStruct
{
    EConnectionType ConnectionType;
    std::optional<std::string> ClusterName;
    TTableMountCacheConfigPtr TableMountCache;
    NChaosClient::TReplicationCardCacheConfigPtr ReplicationCardCache;

    REGISTER_YSON_STRUCT(TConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TConnectionDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    NTabletClient::TTableMountCacheDynamicConfigPtr TableMountCache;

    TExponentialBackoffOptions TabletWriteBackoff;

    REGISTER_YSON_STRUCT(TConnectionDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TPersistentQueuePollerConfig
    : public virtual NYTree::TYsonStruct
{
    //! Try to keep at most this many prefetched rows in memory. This limit is approximate.
    i64 MaxPrefetchRowCount;

    //! Try to keep at most this much prefetched data in memory. This limit is approximate.
    i64 MaxPrefetchDataWeight;

    //! The limit for the number of rows to be requested in a single background fetch request.
    i64 MaxRowsPerFetch;

    //! The limit for the number of rows to be returned by #TPersistentQueuePoller::Poll call.
    i64 MaxRowsPerPoll;

    //! The limit on maximum number of consumed but not yet trimmed row indexes. No new rows are fetched when the limit is reached.
    i64 MaxFetchedUntrimmedRowCount;

    //! When trimming data table, keep the number of consumed but untrimmed rows about this level.
    i64 UntrimmedDataRowsLow;

    //! When more than this many of consumed but untrimmed rows appear in data table, trim the front ones
    //! in accordance to #UntrimmedDataRowsLow.
    i64 UntrimmedDataRowsHigh;

    //! How often the data table is to be polled.
    TDuration DataPollPeriod;

    //! How often the state table is to be trimmed.
    TDuration StateTrimPeriod;

    //! For how long to backoff when a state conflict is detected.
    TDuration BackoffTime;

    REGISTER_YSON_STRUCT(TPersistentQueuePollerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistentQueuePollerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFileReaderConfig
    : public virtual NChunkClient::TMultiChunkReaderConfig
{
    REGISTER_YSON_STRUCT(TFileReaderConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TFileReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFileWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public NFileClient::TFileChunkWriterConfig
{
    REGISTER_YSON_STRUCT(TFileWriterConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TFileWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJournalReaderConfig
    : public NJournalClient::TChunkReaderConfig
    , public TWorkloadConfig
{
    REGISTER_YSON_STRUCT(TJournalReaderConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TJournalReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJournalChunkWriterConfig
    : public virtual TWorkloadConfig
{
    int MaxBatchRowCount;
    i64 MaxBatchDataSize;
    TDuration MaxBatchDelay;

    int MaxFlushRowCount;
    i64 MaxFlushDataSize;

    bool PreferLocalHost;

    TDuration NodeRpcTimeout;
    TDuration NodePingPeriod;
    TDuration NodeBanTimeout;

    NRpc::TRetryingChannelConfigPtr NodeChannel;

    // For testing purposes only.
    double ReplicaFailureProbability;

    //! After writing #ReplicaRowLimits[index] rows to replica #index
    //! request will fail with timeout after #ReplicaFakeTimeoutDelay
    //! but rows will be actually written.
    std::optional<std::vector<int>> ReplicaRowLimits;
    TDuration ReplicaFakeTimeoutDelay;

    REGISTER_YSON_STRUCT(TJournalChunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicJournalWriterConfig
    : public virtual NYTree::TYsonStruct
{
    std::optional<bool> ValidateErasureCoding;

    REGISTER_YSON_STRUCT(TDynamicJournalWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicJournalWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJournalWriterConfig
    : public TJournalChunkWriterConfig
{
    int MaxChunkRowCount;
    i64 MaxChunkDataSize;
    TDuration MaxChunkSessionDuration;

    TDuration OpenSessionBackoffTime;
    int OpenSessionRetryCount;

    TDuration PrerequisiteTransactionProbePeriod;

    bool EnableChecksums;
    bool ValidateErasureCoding;

    // For testing purposes only.
    bool DontClose;
    bool DontSeal;
    bool DontPreallocate;

    std::optional<TDuration> OpenDelay;

    TJournalWriterConfigPtr ApplyDynamic(const TDynamicJournalWriterConfigPtr& dynamicConfig) const;
    void ApplyDynamicInplace(const TDynamicJournalWriterConfigPtr& dynamicConfig);

    REGISTER_YSON_STRUCT(TJournalWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJournalChunkWriterOptions
    : public NYTree::TYsonStruct
{
    int ReplicationFactor;
    NErasure::ECodec ErasureCodec;

    int ReadQuorum;
    int WriteQuorum;

    int ReplicaLagLimit;

    bool EnableMultiplexing;

    REGISTER_YSON_STRUCT(TJournalChunkWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

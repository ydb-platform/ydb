#pragma once

#include "client_common.h"

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/queue_client/queue_rowset.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TPullRowsOptions
    : public TTabletReadOptions
{
    NChaosClient::TReplicaId UpstreamReplicaId;
    THashMap<NTabletClient::TTabletId, i64> StartReplicationRowIndexes;
    i64 TabletRowsPerRead = 1000;
    bool OrderRowsByTimestamp = false;

    NChaosClient::TReplicationProgress ReplicationProgress;
    NTransactionClient::TTimestamp UpperTimestamp = NTransactionClient::NullTimestamp;
    NTableClient::TTableSchemaPtr TableSchema;
    i64 MaxDataWeight = 1_GB;
    IReservingMemoryUsageTrackerPtr MemoryTracker;
};

struct TPullRowsResult
{
    THashMap<NTabletClient::TTabletId, i64> EndReplicationRowIndexes;
    i64 RowCount = 0;
    i64 DataWeight = 0;
    NChaosClient::TReplicationProgress ReplicationProgress;
    ITypeErasedRowsetPtr Rowset;
    bool Versioned = true;
};

struct TPullQueueOptions
    : public TSelectRowsOptions
    , public TFallbackReplicaOptions
{
    // COMPAT(achulkov2): Remove this once we drop support for legacy PullQueue via SelectRows.
    bool UseNativeTabletNodeApi = true;
};

struct TPullQueueConsumerOptions
    : public TPullQueueOptions
{ };

struct TRegisterQueueConsumerOptions
    : public TTimeoutOptions
{
    std::optional<std::vector<int>> Partitions;
};

struct TUnregisterQueueConsumerOptions
    : public TTimeoutOptions
{ };

struct TListQueueConsumerRegistrationsOptions
    : public TTimeoutOptions
{ };

struct TListQueueConsumerRegistrationsResult
{
    NYPath::TRichYPath QueuePath;
    NYPath::TRichYPath ConsumerPath;
    bool Vital;
    std::optional<std::vector<int>> Partitions;
};

struct TCreateQueueProducerSessionOptions
    : public TTimeoutOptions
{
    NYTree::INodePtr UserMeta;
};

struct TCreateQueueProducerSessionResult
{
    NQueueClient::TQueueProducerSequenceNumber SequenceNumber;
    NQueueClient::TQueueProducerEpoch Epoch;
    NYTree::INodePtr UserMeta;
};

struct TRemoveQueueProducerSessionOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IQueueClientBase
{
    virtual ~IQueueClientBase() = default;

    virtual TFuture<TPullRowsResult> PullRows(
        const NYPath::TYPath& path,
        const TPullRowsOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IQueueClient
{
    virtual ~IQueueClient() = default;

    //! Reads a batch of rows from a given partition of a given queue, starting at (at least) the given offset.
    //! Requires the user to have read-access to the specified queue.
    //! There is no guarantee that `rowBatchReadOptions.MaxRowCount` rows will be returned even if they are in the queue.
    virtual TFuture<NQueueClient::IQueueRowsetPtr> PullQueue(
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options = {}) = 0;

    //! Same as PullQueue, but requires user to have read-access to the consumer and the consumer being registered for the given queue.
    //! There is no guarantee that `rowBatchReadOptions.MaxRowCount` rows will be returned even if they are in the queue.
    virtual TFuture<NQueueClient::IQueueRowsetPtr> PullQueueConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        std::optional<i64> offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueConsumerOptions& options = {}) = 0;

    virtual TFuture<void> RegisterQueueConsumer(
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        bool vital,
        const TRegisterQueueConsumerOptions& options = {}) = 0;

    virtual TFuture<void> UnregisterQueueConsumer(
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        const TUnregisterQueueConsumerOptions& options = {}) = 0;

    virtual TFuture<std::vector<TListQueueConsumerRegistrationsResult>> ListQueueConsumerRegistrations(
        const std::optional<NYPath::TRichYPath>& queuePath,
        const std::optional<NYPath::TRichYPath>& consumerPath,
        const TListQueueConsumerRegistrationsOptions& options = {}) = 0;

    virtual TFuture<TCreateQueueProducerSessionResult> CreateQueueProducerSession(
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        const TCreateQueueProducerSessionOptions& options = {}) = 0;

    virtual TFuture<void> RemoveQueueProducerSession(
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        const TRemoveQueueProducerSessionOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi


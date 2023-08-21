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

struct TPullConsumerOptions
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

////////////////////////////////////////////////////////////////////////////////

struct IQueueClientBase
{
    virtual TFuture<TPullRowsResult> PullRows(
        const NYPath::TYPath& path,
        const TPullRowsOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IQueueClient
{
    //! Reads a batch of rows from a given partition of a given queue, starting at (at least) the given offset.
    //! Requires the user to have read-access to the specified queue.
    virtual TFuture<NQueueClient::IQueueRowsetPtr> PullQueue(
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options = {}) = 0;

    //! Same as PullQueue, but requires user to have read-access to the consumer and the consumer being registered for the given queue.
    virtual TFuture<NQueueClient::IQueueRowsetPtr> PullConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullConsumerOptions& options = {}) = 0;

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
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi


#pragma once

#include "public.h"
#include "common.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

struct TConsumerMeta
    : public NYTree::TYsonStructLite
{
    std::optional<i64> CumulativeDataWeight;
    std::optional<ui64> OffsetTimestamp;

    REGISTER_YSON_STRUCT_LITE(TConsumerMeta);

    static void Register(TRegistrar registrar);
};

struct TPartitionInfo
{
    i64 PartitionIndex = -1;
    // TODO(max42): rename into Offset, this seems like a concise name.
    i64 NextRowIndex = -1;
    //! Latest time instant the corresponding partition was consumed.
    TInstant LastConsumeTime;
    std::optional<TConsumerMeta> ConsumerMeta;
};

////////////////////////////////////////////////////////////////////////////////

//! Interface representing a consumer table.
struct ISubConsumerClient
    : public TRefCounted
{
    //! Advance the offset for the given partition, setting it to a new value within the given transaction.
    //!
    //! If oldOffset is specified, the current offset is read (within the transaction) and compared with oldOffset.
    //! If they are equal, the new offset is written, otherwise an exception is thrown.
    virtual void Advance(
        const NApi::ITransactionPtr& consumerTransaction,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset) const = 0;

    //! Collect partition infos. If there are entries in the consumer table with partition
    //! indices outside of range [0, expectedPartitionCount), they will be ignored.
    virtual TFuture<std::vector<TPartitionInfo>> CollectPartitions(
        int expectedPartitionCount,
        bool withLastConsumeTime = false) const = 0;

    //! Collect partition infos.
    //! Entries in the consumer table with partition indices not in the given vector will be ignored.
    virtual TFuture<std::vector<TPartitionInfo>> CollectPartitions(
        const std::vector<int>& partitionIndexes,
        bool withLastConsumeTime = false) const = 0;

    struct TPartitionStatistics
    {
        i64 FlushedDataWeight = 0;
        i64 FlushedRowCount = 0;
    };

    // TODO(achulkov2): This should probably handle multiple partition indexes at some point.
    // TODO(achulkov2): Move this to a separate IQueueClient class?
    //! Fetch relevant per-tablet statistics for queue.
    virtual TFuture<TPartitionStatistics> FetchPartitionStatistics(
        const NYPath::TYPath& queue,
        int partitionIndex) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISubConsumerClient)

struct IConsumerClient
    : public TRefCounted
{
    virtual ISubConsumerClientPtr GetSubConsumerClient(const NApi::IClientPtr& queueClusterClient, const TCrossClusterReference& queueRef) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IConsumerClient)

////////////////////////////////////////////////////////////////////////////////

//! Creates a native YT multi-queue consumer client.
IConsumerClientPtr CreateConsumerClient(
    const NApi::IClientPtr& consumerClusterClient,
    const NYPath::TYPath& consumerPath,
    const NTableClient::TTableSchema& consumerSchema);

//! Uses the table mount cache to fetch the consumer's schema and
//! make sure the consumer actually has YT consumer schema.
IConsumerClientPtr CreateConsumerClient(
    const NApi::IClientPtr& clusterClient,
    const NYPath::TYPath& consumerPath);

//! Uses the table mount cache to fetch the consumer's schema and
//! make sure the consumer actually has YT consumer schema.
//! Uses the given queue path to fetch the corresponding subconsumer.
//! If no cluster is set for queue, it is inferred from the given client.
ISubConsumerClientPtr CreateSubConsumerClient(
    const NApi::IClientPtr& consumerClusterClient,
    const NApi::IClientPtr& queueClusterClient,
    const NYPath::TYPath& consumerPath,
    NYPath::TRichYPath queuePath);

const NTableClient::TTableSchemaPtr& GetConsumerSchema();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

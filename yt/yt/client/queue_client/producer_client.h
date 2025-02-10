#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

struct TProducerSessionBatchOptions
{
    //! Weight of serialized buffered rows when flush will be called regardless of the background flush period.
    std::optional<i64> ByteSize;
    //! Buffered rows count when flush will be called regardless of the background flush period.
    std::optional<i64> RowCount;
};

using TAckCallback = TCallback<void(NQueueClient::TQueueProducerSequenceNumber)>;

struct TProducerSessionOptions
{
    //! If true, sequence numbers will be incremented automatically,
    //! and rows should not contain values of $sequence_number column.
    //! If false, each row should contain value of $sequnce_number column.
    bool AutoSequenceNumber = false;

    //! Batch sizes when rows will be flushed to server regardless of the background flush period (if it is specified).
    //! If there is no background flush, rows will be flush when `BatchOptions::ByteSize` or `BatchOptions::RowCount` is reached. If none of them are specified, `BatchOptions::ByteSize` will be equal to 16 MB.
    TProducerSessionBatchOptions BatchOptions;

    //! If set, rows will be flushed in background with this period.
    std::optional<TDuration> BackgroundFlushPeriod;

    //! Backoff strategy for retries when background flush is turned on.
    TBackoffStrategy BackoffStrategy = TBackoffStrategy(TExponentialBackoffOptions{});

    //! Acknowledgment callback.
    TAckCallback AckCallback;

    //! If this happens to be a push into a replicated table queue,
    //! controls if at least one sync replica is required.
    bool RequireSyncReplica = true;
};

struct IProducerSession
    : public NTableClient::IUnversionedRowsetWriter
{
    //! Get sequence number of last pushed row.
    //! For example, it can be gotten right after creating session
    //! to understand what rows should be written now.
    virtual TQueueProducerSequenceNumber GetLastSequenceNumber() const = 0;

    //! Get user meta saved in the producer session.
    virtual const NYTree::INodePtr& GetUserMeta() const = 0;

    //! Flush all written rows.
    virtual TFuture<void> Flush() = 0;

    //! Cancel writing of all not flushed rows.
    virtual void Cancel() = 0;
};

DEFINE_REFCOUNTED_TYPE(IProducerSession)

////////////////////////////////////////////////////////////////////////////////

struct IProducerClient
    : public virtual TRefCounted
{
    //! Create a session (or increase its epoch) and return session writer.
    virtual TFuture<IProducerSessionPtr> CreateSession(
        const NYPath::TRichYPath& queuePath,
        const NTableClient::TNameTablePtr& nameTable,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        const TProducerSessionOptions& options = {},
        const IInvokerPtr& invoker = nullptr) = 0;
};
DEFINE_REFCOUNTED_TYPE(IProducerClient)

////////////////////////////////////////////////////////////////////////////////

IProducerClientPtr CreateProducerClient(
    const NApi::IClientPtr& client,
    const NYPath::TRichYPath& producerPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

struct TProducerSessionOptions
{
    //! If true, sequence numbers will be incremented automatically,
    //! and rows should not contain values of $sequence_number column.
    //! If false, each row should contain value of $sequnce_number column.
    bool AutoSequenceNumber = false;

    //! Size of buffer when rows will be flushed to server.
    size_t MaxBufferSize = 1_MB;
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
};
DEFINE_REFCOUNTED_TYPE(IProducerSession)

////////////////////////////////////////////////////////////////////////////////

struct IProducerClient
    : public virtual TRefCounted
{
    //! Create a session (or increase its epoch) and return session writer.
    //! NB: Session writer return by this method is NOT thread-safe.
    virtual TFuture<IProducerSessionPtr> CreateSession(
        const NYPath::TRichYPath& queuePath,
        const NTableClient::TNameTablePtr& nameTable,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        const TProducerSessionOptions& options = {}) = 0;
};
DEFINE_REFCOUNTED_TYPE(IProducerClient)

////////////////////////////////////////////////////////////////////////////////

IProducerClientPtr CreateProducerClient(
    const NApi::IClientPtr& client,
    const NYPath::TRichYPath& producerPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

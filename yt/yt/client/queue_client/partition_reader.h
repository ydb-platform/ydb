#pragma once

#include "config.h"
#include "public.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

struct IPersistentQueueRowset
    : public IQueueRowset
{
    //! Stages offset advancement GetStartOffset() -> GetFinishOffset() in the given transaction.
    virtual void Commit(const NApi::ITransactionPtr& transaction) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPersistentQueueRowset)

////////////////////////////////////////////////////////////////////////////////

struct IPartitionReader
    : public TRefCounted
{
    virtual TFuture<void> Open() = 0;
    virtual TFuture<IPersistentQueueRowsetPtr> Read() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPartitionReader)

IPartitionReaderPtr CreateMultiQueueConsumerPartitionReader(
    TPartitionReaderConfigPtr config,
    NApi::IClientPtr client,
    NYPath::TRichYPath consumerPath,
    NYPath::TRichYPath queuePath,
    int partitionIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

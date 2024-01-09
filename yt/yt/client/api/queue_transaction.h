#pragma once

#include "client_common.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TAdvanceConsumerOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IQueueTransaction
{
    virtual ~IQueueTransaction() = default;

    // TODO(nadya73): Remove it: YT-20712
    virtual void AdvanceConsumer(
        const NYPath::TYPath& path,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset) = 0;

    // TODO(nadya73): Remove it: YT-20712
    virtual void AdvanceConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset) = 0;

    //! Advances the consumer's offset for partition with index #partitionIndex, setting it to #newOffset.
    /*!
     *  If #oldOffset is specified, the current offset is read inside this transaction and compared with #oldOffset.
     *  If they are equal, the new offset is written, otherwise an exception is thrown.
     */
    virtual TFuture<void> AdvanceConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset,
        const TAdvanceConsumerOptions& options) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi


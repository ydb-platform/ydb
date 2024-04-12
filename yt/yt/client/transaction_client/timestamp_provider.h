#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Manages cluster-wide unique timestamps.
/*!
 *  Thread affinity: any
 */
struct ITimestampProvider
    : public virtual TRefCounted
{
    //! Generates a contiguous range of timestamps (of size #count)
    //! that are guaranteed to be larger than all timestamps previously obtained via this instance.
    //! Returns the first timestamp of that range.
    virtual TFuture<TTimestamp> GenerateTimestamps(
        int count = 1,
        NObjectClient::TCellTag clockClusterTag = NObjectClient::InvalidCellTag) = 0;

    //! Returns the latest timestamp returned from #GenerateTimestamps.
    virtual TTimestamp GetLatestTimestamp(NObjectClient::TCellTag clockClusterTag = NObjectClient::InvalidCellTag) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITimestampProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient


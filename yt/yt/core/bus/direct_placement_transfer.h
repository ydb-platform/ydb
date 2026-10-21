#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

#include <vector>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

//! A pending transfer whose destination buffers are supplied by the consumer.
/*!
 *  The producer announces, via #GetExpectedBufferSizes, the per-part layout it is
 *  going to fill; the consumer supplies matching buffers via #Run, into which the
 *  payload is placed directly (no intermediate staging). #Run returns the
 *  reassembled message built from those buffers. Supplying the destination lets the
 *  consumer decide where the data lands — e.g. into registered, pinned or device
 *  memory, potentially enabling a true zero-copy receive.
 *
 *  Null parts are preserved: the bus guarantees that a part that was null on the
 *  sending side is delivered as a null ref (distinct from a zero-size part). The
 *  transport remembers which parts were null and restores them in #Run.
 */
struct IDirectPlacementTransfer
    : public virtual TRefCounted
{
    //! Returns the byte size of each part, in order. A null part or a non-null
    //! empty part is reported as size zero (the two are indistinguishable here);
    //! #Run nonetheless restores null parts as null refs.
    virtual std::vector<i64> GetExpectedBufferSizes() const = 0;

    //! Places the data into the supplied buffers, taking ownership of them.
    /*!
     *  #buffers must contain exactly one entry per part, each of the size reported
     *  by #GetExpectedBufferSizes (a zero-size buffer for a null or empty part).
     *  Ownership is transferred to the transfer, which keeps the buffers alive until
     *  the returned future is set.
     *
     *  Must be invoked exactly once. It may be called synchronously or deferred and
     *  called later from an arbitrary thread.
     *
     *  \returns a future to the reassembled parts, set once every buffer has been
     *  filled: each part aliases its supplied buffer, except a null part, which is
     *  restored as a null ref. Canceling the returned future aborts the transfer;
     *  likewise, destroying the transfer without ever calling #Run aborts it.
     */
    virtual TFuture<std::vector<TSharedRef>> Run(std::vector<TSharedMutableRef>&& buffers) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDirectPlacementTransfer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

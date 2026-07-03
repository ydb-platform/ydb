#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

#include <vector>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! RPC-layer counterpart of #NBus::IDirectPlacementTransfer.
/*!
 *  Mimics the bus interface, but #Run returns just a #TFuture<void>: unlike at the
 *  bus layer, the consumer does not receive the reassembled parts from #Run.
 *  Instead, once the transfer completes, the attachments become available through
 *  the owner of the transfer, namely #IServiceContext::TryGetRequestAttachmentsTransfer
 *  (then #IServiceContext::RequestAttachments) on the server side and
 *  #TClientResponse::TryGetResponseAttachmentsTransfer (then the response's
 *  attachments) on the client side.
 *
 *  See #NBus::IDirectPlacementTransfer for the buffer/null-part semantics.
 */
struct IDirectPlacementTransfer
    : public virtual TRefCounted
{
    //! Same contract as #NBus::IDirectPlacementTransfer::GetExpectedBufferSizes.
    virtual std::vector<i64> GetExpectedBufferSizes() const = 0;

    //! Places the data into the supplied buffers, taking ownership of them, and
    //! makes the attachments available on the owner once the returned future is set.
    /*!
     *  Must be invoked exactly once. See #NBus::IDirectPlacementTransfer::Run.
     */
    virtual TFuture<void> Run(std::vector<TSharedMutableRef>&& buffers) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDirectPlacementTransfer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

//! Handles incoming bus messages.
struct IMessageHandler
    : public virtual TRefCounted
{
    //! Raised whenever a new message arrives via the bus.
    /*!
     *  \param message The just arrived message. If the sender requested
     *  direct placement of some trailing parts (see
     *  #TSendOptions::DirectPlacementTransferPartCount) and the transport supports
     *  it, those parts are omitted here and must be fetched via #transfer.
     *  \param replyBus A bus that can be used for replying back.
     *  \param transfer A handle for fetching the deferred parts, or null if the
     *  message has none (or the transport delivered them inline). When non-null,
     *  the receiver must drive it to completion (see #IDirectPlacementTransfer).
     *
     *  \note
     *  Thread affinity: this method is called from an unspecified thread
     *  and must return ASAP. No context switch or fiber cancelation is possible.
     *  In particular, do not block waiting for #transfer to complete; either run
     *  it synchronously or retain the handle and complete it later.
     */
    virtual void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus,
        IDirectPlacementTransferPtr transfer = nullptr) noexcept = 0;
};

DEFINE_REFCOUNTED_TYPE(IMessageHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

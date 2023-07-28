#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

//! A server-side bus listener.
/*!
 *  An instance on this interface listens for incoming
 *  messages and notifies IMessageHandlerPtr.
 */
struct IBusServer
    : public virtual TRefCounted
{
    //! Synchronously starts the listener.
    /*
     *  \param handler Incoming messages handler.
     */
    virtual void Start(IMessageHandlerPtr handler) = 0;

    //! Asynchronously stops the listener.
    /*!
     *  After this call the instance is no longer usable.
     *  No new incoming messages are accepted.
     *
     *  \returns the future indicating the moment when the server is fully stopped;
     *  e.g. the server socket is closed.
     */
    virtual TFuture<void> Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IBusServer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

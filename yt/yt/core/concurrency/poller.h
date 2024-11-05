#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/proc.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Describes the types of events a pollable entity may be interested in.
DEFINE_BIT_ENUM(EPollControl,
    ((None)         (0x0))
    ((Read)         (0x1))      // Pending read (OnEvent, Arm)
    ((Write)        (0x2))      // Pending write (OnEvent, Arm)
    ((Retry)        (0x8))      // Retry requested (OnEvent)
    ((EdgeTriggered)(0x10))     // TODO(khlebnikov) make it default (Arm)
    ((ReadHup)      (0x20))
    ((BacklogEmpty) (0x40))     // Socket is consumed for now (used for optimization purposes)
    ((Offline)      (0x80))     // Cannot handle events  (for external use)
    ((Terminate)    (0x100))    // Termination requested (for external use)
    ((Running)      (0x200))    // Operation in progress (for external use)
    ((Shutdown)     (0x400))    // Shutdown in progress  (for external use)
);

////////////////////////////////////////////////////////////////////////////////

//! Describes an FD-backed pollable entity.
struct IPollable
    : public virtual TRefCounted
{
    //! Cookie is an opaque ref-counted object that could be attached to a pollable by its poller.
    using TCookiePtr = TRefCountedPtr;

    //! Attaches a cookie.
    virtual void SetCookie(TCookiePtr cookie) = 0;

    //! Returns the attached (type-erased) cookie.
    virtual void* GetCookie() const = 0;

    //! Returns a human-readable string used for diagnostic purposes.
    virtual const TString& GetLoggingTag() const = 0;

    //! Called by the poller when the appropriate event is triggered for the FD.
    virtual void OnEvent(EPollControl control) = 0;

    //! Called by the poller when the pollable entity is unregistered.
    virtual void OnShutdown() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPollable)

////////////////////////////////////////////////////////////////////////////////

//! Enables polling a collection of IPollable-s.
/*!
 *  A poller is typically implemented as a thread or a pool of threads running
 *  a polling loop.
 *
 *  Additionally a poller provides means to execute arbitrary callbacks via IInvoker interface,
 *  \see IPoller::GetInvoker.
 */
struct IPoller
    : public virtual TRefCounted
{
    //! Shuts the poller down; e.g. reliably terminates the threads.
    //! The poller is not longer usable after this call.
    virtual void Shutdown() = 0;

    //! Tries to register a pollable entity but does not arm the poller yet.
    //! Returns |false| if the poller is already shutting down.
    virtual bool TryRegister(const IPollablePtr& pollable, TString poolName = "default") = 0;

    //! Method must be called inside OnEvent.
    virtual void SetExecutionPool(const IPollablePtr& pollable, TString poolName) = 0;

    //! Unregisters the previously registered entity.
    /*!
     *  If the pollable entity was previously armed, one should unarm it first
     *  before unregistering. Not doing so is OK for the poller, however
     *  in this case #IPollable::OnShutdown and #IPollable::OnEvent could be invoked concurrently.
     *
     *  At the same time, if the poller was properly unarmed before unregistering,
     *  it is guaranteed that #IPollable::OnShutdown and #IPollable::OnEvent will
     *  not be run concurrently.
     *
     *  The entity gets unregistered asynchronously.
     *
     *  \returns a future that is set when the entity becomes unregistered
     *  (after #IPollable::OnShutdown is invoked)
     */
    virtual TFuture<void> Unregister(const IPollablePtr& pollable) = 0;

    //! Arms the poller to handle events of a given type for a given entity.
    //! Can be called multiple times if one-shot mode is used.
    virtual void Arm(TFileDescriptor fd, const IPollablePtr& pollable, EPollControl control) = 0;

    //! Schedule call of #IPollable::OnEvent with EPollControl::Retry.
    virtual void Retry(const IPollablePtr& pollable) = 0;

    //! Unarms the poller.
    virtual void Unarm(TFileDescriptor fd, const IPollablePtr& pollable) = 0;

    //! Returns the invoker capable of executing arbitrary callbacks
    //! in the poller's context.
    virtual IInvokerPtr GetInvoker() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IPoller)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

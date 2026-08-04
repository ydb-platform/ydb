#pragma once

#include "public.h"
#include "future.h"
#include "signal.h"

#include <library/cpp/yt/memory/weak_ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Maintains a flag indicating if the context is canceled.
//! Propagates cancelation to other contexts and futures.
/*!
 *  \note
 *  Thread-affinity: any
 */
class TCancelableContext
    : public TRefCounted
{
public:
    //! Returns |true| iff the context is canceled.
    bool IsCanceled() const;

    //! Only safe to use after IsCanceled returned |true|.
    const TError& GetCancelationError() const;

    //! Marks the context as canceled raising the handlers
    //! and propagates cancelation.
    void Cancel(const TError& error);

    //! Raised when the context is canceled.
    DECLARE_SIGNAL(void(const TError&), Canceled);

    //! Registers another context for propagating cancelation.
    void PropagateTo(const TCancelableContextPtr& context);

    //! Registers a future for propagating cancelation.
    template <class T>
    void PropagateTo(const TFuture<T>& future);
    void PropagateTo(const TFuture<void>& future);

    //! Creates a new invoker wrapping the existing one.
    /*!
     *  Callbacks are executed by the underlying invoker as long as the context
     *  is not canceled. Double check is employed: the first one happens
     *  at the instant the callback is enqueued and the second one -- when
     *  the callback starts executing.
     */
    IInvokerPtr CreateInvoker(IInvokerPtr underlyingInvoker);

private:
    class TCancelableInvoker;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::atomic<bool> Canceled_ = false;
    TError CancelationError_;
    TSingleShotCallbackList<void(const TError&)> Handlers_;
    THashSet<TWeakPtr<TCancelableContext>> PropagateToContexts_;
    THashSet<TFuture<void>> PropagateToFutures_;
};

DEFINE_REFCOUNTED_TYPE(TCancelableContext)

////////////////////////////////////////////////////////////////////////////////

//! Returns the cancelable context installed for the current fiber (or null if none).
TCancelableContext* TryGetCurrentCancelableContext();
//! Installs #newContext for the current fiber, returning the previously installed one.
TCancelableContextPtr SwitchCurrentCancelableContext(TCancelableContextPtr newContext);

//! Installs the given context into the current fiber slot for the guard's lifetime.
//! Move-only; a moved-from guard restores nothing.
class TCurrentCancelableContextGuard
{
public:
    explicit TCurrentCancelableContextGuard(TCancelableContextPtr context);
    TCurrentCancelableContextGuard(TCurrentCancelableContextGuard&& other) noexcept;

    TCurrentCancelableContextGuard(const TCurrentCancelableContextGuard&) = delete;
    TCurrentCancelableContextGuard& operator=(const TCurrentCancelableContextGuard&) = delete;
    TCurrentCancelableContextGuard& operator=(TCurrentCancelableContextGuard&&) = delete;

    ~TCurrentCancelableContextGuard();

private:
    bool Active_;
    TCancelableContextPtr OldContext_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CANCELABLE_CONTEXT_INL_H_
#include "cancelable_context-inl.h"
#undef CANCELABLE_CONTEXT_INL_H_

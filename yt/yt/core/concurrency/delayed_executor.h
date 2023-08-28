#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Manages delayed callback execution.
class TDelayedExecutor
{
public:
    using TDelayedCallback = TCallback<void(bool)>;

    //! Constructs a future that gets set when a given #delay elapses.
    /*!
     *  \param delay Delay after which the returned future is set.
     *  \param invoker Invoker where the future becomes set (DelayedExecutor if null).
     *  Note that during shutdown this future will be resolved prematurely with an error.
     */
    static TFuture<void> MakeDelayed(
        TDuration delay,
        IInvokerPtr invoker = nullptr);

    //! Constructs a future that gets set when a given #duration elapses and
    //! immediately waits for it.
    /*!
     *  This is barely equivalent to MakeDelayed and WaitFor combination.
     *  The result of waiting is ignored.
     */
    static void WaitForDuration(TDuration duration);

    //! Submits #callback for execution after a given #delay.
    /*!
     *  #callback is guaranteed to be invoked exactly once unless the cookie was cancelled (cf. #Cancel).
     *  The exact thread where the invocation takes place is unspecified.
     *
     *  |aborted| flag is provided to the callback to indicate whether this is a premature execution
     *  due to shutdown or not.
     *
     *  Note that after shutdown has been initiated, #Submit may start invoking the newly-passed callbacks
     *  immediately with |aborted = true|. It is guaranteed, though, that each #Submit call may only
     *  cause an immediate execution of *its* callback but not others.
     *
     *  \param callback A callback to execute.
     *  \param delay Execution delay.
     *  \param invoker Invoker that will be handling #callback (DelayedExecutor if null).
     *  \return An opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(
        TDelayedCallback callback,
        TDuration delay,
        IInvokerPtr invoker = nullptr);

    //! Submits #closure for execution after a given #delay.
    /*!
     *  \param closure A closure to execute.
     *  \param delay Execution delay.
     *  \param invoker Invoker that will be handling #callback (DelayedExecutor if null).
     *  \return An opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(
        TClosure closure,
        TDuration delay,
        IInvokerPtr invoker = nullptr);

    //! Submits #callback for execution at a given #deadline.
    /*!
     * \param callback A callback to execute.
     * \param deadline Execution deadline.
     * \param invoker Invoker that will be handling #callback (DelayedExecutor if null).
     * \return An opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(
        TDelayedCallback callback,
        TInstant deadline,
        IInvokerPtr invoker = nullptr);

    //! Submits #closure for execution at a given #deadline.
    /*!
     * \param closure A closure to execute.
     * \param delay Execution deadline.
     * \param invoker Invoker that will be handling #callback (DelayedExecutor if null).
     * \return An opaque cancelation cookie.
     */
    static TDelayedExecutorCookie Submit(
        TClosure closure,
        TInstant deadline,
        IInvokerPtr invoker = nullptr);

    //! Cancels an earlier scheduled execution.
    /*!
     *  This call is "safe", i.e. cannot lead to immediate execution of any callbacks even
     *  during shutdown.
     *
     *  Cancelation should always be regarded as a hint. It is inherently racy and
     *  is not guaranteed to be handled during and after shutdown.
     */
    static void Cancel(const TDelayedExecutorCookie& cookie);

    //! The same as Cancel, but in addition also clears the cookie.
    static void CancelAndClear(TDelayedExecutorCookie& cookie);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

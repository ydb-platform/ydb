#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Helps to remember previously served requests thus enabling client-side retries
//! even for non-idempotent actions.
/*!
 *  Clients assign a unique (random) mutation id to every retry session.
 *  Servers ignore requests whose mutation ids are already known.
 *
 *  After a sufficiently long period of time, a remembered response gets evicted.
 *
 *  The keeper is initially inactive.
 *
 *  \note
 *  Thread affinity: single-threaded (unless noted otherwise)
 */
struct IResponseKeeper
    : public TRefCounted
{
public:
    virtual void Start() = 0;

    //! Deactivates the keeper.
    /*!
     *  If the keeper is already stopped, the call does nothing.
     *  Calling #TryBeginRequest for an inactive keeper will lead to an exception.
     */
    virtual void Stop() = 0;

    //! Called upon receiving a request with a given mutation #id.
    /*!
     *  Either returns a valid future for the response (which can either be unset
     *  if the request is still being served or set if it is already completed) or
     *  a null future if #id is not known. In the latter case subsequent
     *  calls to #TryBeginRequest will be returning the same future over and
     *  over again.
     *
     *  The call throws if the keeper is not active or if #isRetry is |true| and
     *  the keeper is warming up.
     */
    virtual TFuture<TSharedRefArray> TryBeginRequest(TMutationId id, bool isRetry) = 0;

    //! Same as above but does not change the state of response keeper.
    //! That is, if a null future is returned, no pending future has been created for it.
    virtual TFuture<TSharedRefArray> FindRequest(TMutationId id, bool isRetry) const = 0;

    //! Called when a request with a given mutation #id is finished and a #response is ready.
    /*
     *  If #remember is true, the latter #response is remembered and returned by
     *  future calls to #TryBeginRequest.
     *  Additionally, a call to the returned function object will push #response
     *  to every subscriber waiting for the future previously returned by
     *  #TryBeginRequest. Such a call must be done, or the subscribers will get
     *  a 'promise abandoned' error.
     *  NB: the returned function object may be null if there weren't any
     *  requests associated with #mutationId (or if response keeper isn't started).
     */
    [[nodiscard]]
    virtual std::function<void()> EndRequest(TMutationId id, TSharedRefArray response, bool remember = true) = 0;

    //! Similar to its non-error counterpart but also accepts errors.
    //! Note that these are never remembered and are just propagated to the listeners.
    [[nodiscard]]
    virtual std::function<void()> EndRequest(TMutationId id, TErrorOr<TSharedRefArray> responseOrError, bool remember = true) = 0;

    //! Forgets all pending requests, which were previously registered via #TryBeginRequest.
    virtual void CancelPendingRequests(const TError& error) = 0;

    //! Combines #TryBeginRequest and #EndBeginRequest.
    /*!
     *  If |true| is returned then the request (given by #context) has mutation id assigned and
     *  a previously-remembered response is known. In this case #TryReplyFrom replies #context;
     *  no further server-side processing is needed.
     *
     *  If |false| is returned then either the request has no mutation id assigned or
     *  this id hasn't been seen before. In both cases the server must proceed with serving the request.
     *  Also, if #subscribeToResponse is set and the request has mutation id assigned the response will
     *  be automatically remembered when #context is replied.
     */
    virtual bool TryReplyFrom(
        const IServiceContextPtr& context,
        bool subscribeToResponse = true) = 0;

    //! Returns |true| if the keeper is still warming up.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual bool IsWarmingUp() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

IResponseKeeperPtr CreateResponseKeeper(
    TResponseKeeperConfigPtr config,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

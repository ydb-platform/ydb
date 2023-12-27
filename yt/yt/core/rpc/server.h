#pragma once

#include "public.h"

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Represents a bunch of RPC services listening at a single endpoint.
/*!
 *  Each service is keyed by TServiceId returned by #IService::GetServiceId.
 */
struct IServer
    : public virtual TRefCounted
{
    //! Adds a new #service.
    virtual void RegisterService(IServicePtr service) = 0;

    //! Removes a previously registered #service.
    //! Returns |true| if #service was indeed registered, |false| otherwise.
    virtual bool UnregisterService(IServicePtr service) = 0;

    //! Finds a service by id, returns null if none is found.
    virtual IServicePtr FindService(const TServiceId& serviceId) const = 0;

    //! Finds a service by id, throws an appropriate error if none is found.
    virtual IServicePtr GetServiceOrThrow(const TServiceId& serviceId) const = 0;

    //! Reconfigures the server on-the-fly.
    virtual void Configure(const TServerConfigPtr& config) = 0;

    virtual void OnDynamicConfigChanged(const TServerDynamicConfigPtr& config) = 0;

    //! Starts the server.
    /*!
     *  All requests coming to the server before it is started are immediately rejected.
     */
    virtual void Start() = 0;

    //! Stops the server.
    /*!
     *  If #graceful is |true|, asynchronously waits for all services to stop
     *  (cf. #IService::Stop).
     *
     *  For servers bound to certain transport layer, this call also ensures
     *  that the transport is fully stopped all well.
     *
     *  \returns an asynchronous flag indicating that the server is fully stopped.
     */
    virtual TFuture<void> Stop(bool graceful = true) = 0;
};

DEFINE_REFCOUNTED_TYPE(IServer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

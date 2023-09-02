#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ITicketAuthenticator
    : public virtual TRefCounted
{
    virtual TFuture<TAuthenticationResult> Authenticate(
        const TTicketCredentials& credentials) = 0;

    virtual TFuture<TAuthenticationResult> Authenticate(
        const TServiceTicketCredentials& credentials) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITicketAuthenticator)

////////////////////////////////////////////////////////////////////////////////

ITicketAuthenticatorPtr CreateBlackboxTicketAuthenticator(
    TBlackboxTicketAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackboxService,
    ITvmServicePtr tvmService);

NRpc::IAuthenticatorPtr CreateTicketAuthenticatorWrapper(
    ITicketAuthenticatorPtr underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

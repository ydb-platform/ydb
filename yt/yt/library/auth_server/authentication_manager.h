#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct IAuthenticationManager
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual const NRpc::IAuthenticatorPtr& GetRpcAuthenticator() const = 0;
    virtual const ITokenAuthenticatorPtr& GetTokenAuthenticator() const = 0;
    virtual const ICookieAuthenticatorPtr& GetCookieAuthenticator() const = 0;
    virtual const ITicketAuthenticatorPtr& GetTicketAuthenticator() const = 0;

    virtual const ITvmServicePtr& GetTvmService() const = 0;

    virtual const ICypressCookieManagerPtr& GetCypressCookieManager() const = 0;
    virtual const ICypressUserManagerPtr& GetCypressUserManager() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IAuthenticationManager)

////////////////////////////////////////////////////////////////////////////////

IAuthenticationManagerPtr CreateAuthenticationManager(
    TAuthenticationManagerConfigPtr config,
    NConcurrency::IPollerPtr poller,
    NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

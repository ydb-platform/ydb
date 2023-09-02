#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ICookieAuthenticator
    : public virtual TRefCounted
{
    //! Returns list of cookie names which are used to authentication.
    virtual const std::vector<TStringBuf>& GetCookieNames() const = 0;

    //! Returns true if user provided enough cookies to perform authentication.
    virtual bool CanAuthenticate(const TCookieCredentials& credentials) const = 0;

    virtual TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICookieAuthenticator)

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateCachingCookieAuthenticator(
    TCachingCookieAuthenticatorConfigPtr config,
    ICookieAuthenticatorPtr authenticator,
    NProfiling::TProfiler profiler = {});

ICookieAuthenticatorPtr CreateCompositeCookieAuthenticator(
    std::vector<ICookieAuthenticatorPtr> authenticators);

////////////////////////////////////////////////////////////////////////////////

NRpc::IAuthenticatorPtr CreateCookieAuthenticatorWrapper(
    ICookieAuthenticatorPtr underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ITokenAuthenticator
    : public virtual TRefCounted
{
    virtual TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITokenAuthenticator)

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateBlackboxTokenAuthenticator(
    TBlackboxTokenAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackboxService,
    NProfiling::TProfiler profiler = {});

// This authenticator was created before simple authentication scheme
// and should be removed one day.
ITokenAuthenticatorPtr CreateLegacyCypressTokenAuthenticator(
    TCypressTokenAuthenticatorConfigPtr config,
    NApi::IClientPtr client);

ITokenAuthenticatorPtr CreateCachingTokenAuthenticator(
    TCachingTokenAuthenticatorConfigPtr config,
    ITokenAuthenticatorPtr authenticator,
    NProfiling::TProfiler profiler = {});

ITokenAuthenticatorPtr CreateCompositeTokenAuthenticator(
    std::vector<ITokenAuthenticatorPtr> authenticators);

ITokenAuthenticatorPtr CreateNoopTokenAuthenticator();

NRpc::IAuthenticatorPtr CreateTokenAuthenticatorWrapper(
    ITokenAuthenticatorPtr underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

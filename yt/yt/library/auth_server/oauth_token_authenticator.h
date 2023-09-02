#pragma once

#include "public.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateOAuthTokenAuthenticator(
    TOAuthTokenAuthenticatorConfigPtr config,
    IOAuthServicePtr oauthService,
    ICypressUserManagerPtr userManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

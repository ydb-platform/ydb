#pragma once

#include "public.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateOAuthCookieAuthenticator(
    TOAuthCookieAuthenticatorConfigPtr config,
    IOAuthServicePtr oauthService,
    ICypressUserManagerPtr userManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

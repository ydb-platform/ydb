#pragma once

#include "public.h"

#include <yt/yt/library/auth/authentication_options.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

//! Fills client options from environment variable (client options is permanent for whole lifecycle of program).
/*!
 *  UserName is extracted from YT_USER env variable or uses current system username.
 *  Token is extracted from YT_TOKEN env variable or from file `~/.yt/token`.
 */
TClientOptions GetClientOpsFromEnv();

//! Resolves options only once per launch and then returns the cached result.
const TClientOptions& GetClientOpsFromEnvStatic();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:::NApi

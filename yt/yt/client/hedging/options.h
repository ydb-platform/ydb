#pragma once

#include <yt/yt/client/api/connection.h>

namespace NYT::NClient::NHedging {

////////////////////////////////////////////////////////////////////////////////

//! Fill client options from environment variable (client options is permanent for whole lifecycle of program).
// UserName is extracted from YT_USER env variable or uses current system username.
// Token is extracted from YT_TOKEN env variable or from file `~/.yt/token`.
NApi::TClientOptions GetClientOpsFromEnv();

//! Resolves options only once per launch and then returns the cached result.
const NApi::TClientOptions& GetClientOpsFromEnvStatic();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging

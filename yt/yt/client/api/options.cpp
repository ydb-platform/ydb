#include "options.h"

#include <yt/yt/library/auth/auth.h>

#include <util/string/strip.h>

#include <util/system/env.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

NApi::TClientOptions GetClientOptionsFromEnv()
{
    NApi::TClientOptions options {
        .Token = NAuth::LoadToken(),
    };

    auto user = Strip(GetEnv("YT_USER"));
    if (!user.empty()) {
        options.User = user;
    }

    return options;
}

const NApi::TClientOptions& GetClientOptionsFromEnvStatic()
{
    static const NApi::TClientOptions options = GetClientOptionsFromEnv();
    return options;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

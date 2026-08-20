#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <memory>
#include <string>

namespace NYdb::NConsoleClient {

    std::shared_ptr<IOidcTokenCache> CreateOidcFileTokenCache(const std::string& path);
    std::shared_ptr<IOidcAuthorizationAcceptor> CreateOidcConsoleAcceptor();
    TOidcConfig ReadOidcConfig(const std::string& configPath);

} // namespace NYdb::NConsoleClient

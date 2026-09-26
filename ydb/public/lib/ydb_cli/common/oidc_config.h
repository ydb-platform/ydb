#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <memory>
#include <string>

namespace NYdb::NConsoleClient {

// Loads and validates an OIDC YAML configuration. If cache_path is present,
// its relative form is resolved against the configuration file directory.
NOidc::TOidcConfig LoadOidcConfig(const std::string& configFilePath);

// Creates a provider factory from an OIDC YAML configuration. The optional
// acceptor is used by Device Authorization Grant when user interaction is
// required. Pass nullptr when interaction is not needed.
std::shared_ptr<ICredentialsProviderFactory> CreateOidcFileCredentialsProviderFactory(
    const std::string& configFilePath,
    std::shared_ptr<NOidc::IAuthAcceptor> acceptor);

} // namespace NYdb::NConsoleClient

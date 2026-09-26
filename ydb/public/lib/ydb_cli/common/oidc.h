#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <util/generic/fwd.h>
#include <util/stream/fwd.h>

namespace NYdb::NConsoleClient {

struct TOidcCliOptions;

// The output stream must outlive the acceptor.
std::shared_ptr<NOidc::IAuthAcceptor> CreateCliAuthAcceptor(IOutputStream& output);
std::shared_ptr<ICredentialsProviderFactory> CreateCliOidcCredentialsProviderFactory(const TString& configPath);
std::shared_ptr<ICredentialsProviderFactory> CreateCliOidcCredentialsProviderFactory(const TOidcCliOptions& options);

} // namespace NYdb::NConsoleClient

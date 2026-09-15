#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <util/generic/fwd.h>

namespace NYdb::NConsoleClient {

std::shared_ptr<ICredentialsProviderFactory> CreateCliOidcCredentialsProviderFactory(const TString& configPath);

} // namespace NYdb::NConsoleClient

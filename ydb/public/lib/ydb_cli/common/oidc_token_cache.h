#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <functional>
#include <memory>
#include <string>

namespace NYdb::NConsoleClient {

// Creates a versioned, identity-bound token cache. Writes use an owner-only
// temporary file and atomic replacement. Read and Write are thread-safe;
// no interprocess synchronization is performed. Failures are reported to stderr
// before being rethrown, since the SDK treats cache errors as nonfatal.
std::shared_ptr<NOidc::ITokenCacher> CreateFileTokenCacher(
    const std::string& cacheFilePath,
    const std::string& identity);

// The diagnostic callback receives sanitized errors without token contents or
// identity values. It is owned by the cacher and may run on SDK worker threads.
std::shared_ptr<NOidc::ITokenCacher> CreateFileTokenCacher(
    const std::string& cacheFilePath,
    const std::string& identity,
    std::function<void(const std::string&)> diagnostic);

} // namespace NYdb::NConsoleClient

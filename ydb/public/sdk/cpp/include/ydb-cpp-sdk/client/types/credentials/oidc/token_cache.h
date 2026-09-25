#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <memory>
#include <string>

namespace NYdb::inline Dev::NOidc {

// Creates a versioned, identity-bound token cache. Writes use an owner-only
// temporary file and atomic replacement. Read and Write are thread-safe;
// no interprocess synchronization is performed.
std::shared_ptr<ITokenCacher> CreateFileTokenCacher(
    const std::string& cacheFilePath,
    const std::string& identity);

} // namespace NYdb::inline Dev::NOidc

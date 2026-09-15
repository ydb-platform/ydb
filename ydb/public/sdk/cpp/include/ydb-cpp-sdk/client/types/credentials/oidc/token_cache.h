#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <memory>
#include <string>

namespace NYdb::inline Dev {

// Creates a versioned, identity-bound token cache. Writes use an owner-only
// temporary file and atomic replacement. The returned object also implements
// ILockingTokenCacher for cooperative cross-process refresh transactions.
std::shared_ptr<ITokenCacher> CreateFileTokenCacher(
    const std::string& cacheFilePath,
    const std::string& identity);

} // namespace NYdb::inline Dev

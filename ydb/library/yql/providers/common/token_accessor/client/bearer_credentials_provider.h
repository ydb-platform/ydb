#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

namespace NYql {

std::shared_ptr<NYdb::ICredentialsProviderFactory> WrapCredentialsProviderFactoryWithBearer(
    std::shared_ptr<NYdb::ICredentialsProviderFactory> delegatee
);

}

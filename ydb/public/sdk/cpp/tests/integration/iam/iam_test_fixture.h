#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

namespace NYdb::NTest {

TDriverConfig MakeDriverConfig(TCredentialsProviderFactoryPtr factory);

TStatus RunSelect1Status(TDriver& driver);
void RunSelect1ExpectSuccess(TDriver& driver);

bool IsAuthError(const TStatus& status);

} // namespace NYdb::NTest

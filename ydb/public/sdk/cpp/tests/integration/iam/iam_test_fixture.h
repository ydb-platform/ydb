#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

namespace NYdb::NTest {

TDriverConfig MakeDriverConfig(TCredentialsProviderFactoryPtr factory);

void RunSelect1(TDriver& driver);

} // namespace NYdb::NTest

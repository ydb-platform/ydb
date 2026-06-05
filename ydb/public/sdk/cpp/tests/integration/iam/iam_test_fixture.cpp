#include "iam_test_fixture.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYdb::NTest {

namespace {

void SkipIfNoYdbEnv() {
    const char* endpoint = std::getenv("YDB_ENDPOINT");
    if (!endpoint || !*endpoint) {
        GTEST_SKIP() << "YDB_ENDPOINT is not set";
    }
    const char* database = std::getenv("YDB_DATABASE");
    if (!database || !*database) {
        GTEST_SKIP() << "YDB_DATABASE is not set";
    }
}

} // namespace

TDriverConfig MakeDriverConfig(TCredentialsProviderFactoryPtr factory) {
    SkipIfNoYdbEnv();
    return TDriverConfig()
        .SetEndpoint(std::getenv("YDB_ENDPOINT"))
        .SetDatabase(std::getenv("YDB_DATABASE"))
        .SetCredentialsProviderFactory(std::move(factory));
}

void RunSelect1(TDriver& driver) {
    NQuery::TQueryClient client(driver);
    auto status = client.ExecuteQuery("SELECT 1", NQuery::TTxControl::NoTx()).GetValueSync();
    ASSERT_TRUE(status.IsSuccess()) << status.GetIssues().ToString();
}

} // namespace NYdb::NTest

#include "iam_test_fixture.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace NYdb::NTest {

namespace {

struct TYdbEnv {
    std::string Endpoint;
    std::string Database;
};

TYdbEnv RequireYdbEnvImpl() {
    const char* endpoint = std::getenv("YDB_ENDPOINT");
    if (!endpoint || !*endpoint) {
        throw std::runtime_error(
            "YDB_ENDPOINT is not set; recipe-backed integration target must provide it");
    }
    const char* database = std::getenv("YDB_DATABASE");
    if (!database || !*database) {
        throw std::runtime_error(
            "YDB_DATABASE is not set; recipe-backed integration target must provide it");
    }
    return TYdbEnv{endpoint, database};
}

} // namespace

TDriverConfig MakeDriverConfig(TCredentialsProviderFactoryPtr factory) {
    const TYdbEnv env = RequireYdbEnvImpl();
    return TDriverConfig()
        .SetEndpoint(env.Endpoint)
        .SetDatabase(env.Database)
        .SetCredentialsProviderFactory(std::move(factory));
}

TStatus RunSelect1Status(TDriver& driver) {
    NQuery::TQueryClient client(driver);
    return client.ExecuteQuery("SELECT 1", NQuery::TTxControl::NoTx()).GetValueSync();
}

void RunSelect1ExpectSuccess(TDriver& driver) {
    const auto status = RunSelect1Status(driver);
    ASSERT_TRUE(status.IsSuccess()) << status.GetIssues().ToString();
}

bool IsAuthError(const TStatus& status) {
    const EStatus code = status.GetStatus();
    return code == EStatus::CLIENT_UNAUTHENTICATED || code == EStatus::UNAUTHORIZED;
}

void AssertAuthFailure(TCredentialsProviderFactoryPtr factory, const char* context) {
    TDriver driver(MakeDriverConfig(std::move(factory)));
    const auto status = RunSelect1Status(driver);
    ASSERT_FALSE(status.IsSuccess())
        << "Expected auth failure with " << context << ", but query succeeded. "
        << "Recipe must set YDB_ENFORCE_USER_TOKEN_REQUIREMENT=true.";
    EXPECT_TRUE(IsAuthError(status)) << status.GetIssues().ToString();
}

} // namespace NYdb::NTest

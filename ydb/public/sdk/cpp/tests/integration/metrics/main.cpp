#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/tests/common/fake_metric_registry.h>
#include <util/string/cast.h>

#include <cstdlib>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTests;

namespace {

std::string GetEnvOrEmpty(const char* name) {
    const char* value = std::getenv(name);
    return value ? std::string(value) : std::string();
}

struct TRunArgs {
    TDriver Driver;
    std::shared_ptr<TFakeMetricRegistry> Registry;
    std::string Database;
};

TRunArgs MakeRunArgs() {
    std::string endpoint = GetEnvOrEmpty("YDB_ENDPOINT");
    std::string database = GetEnvOrEmpty("YDB_DATABASE");

    auto registry = std::make_shared<TFakeMetricRegistry>();

    auto driverConfig = TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(GetEnvOrEmpty("YDB_TOKEN"))
        .SetMetricRegistry(registry);

    TDriver driver(driverConfig);
    return {std::move(driver), registry, std::move(database)};
}

std::shared_ptr<TFakeCounter> GetCounter(
    const std::shared_ptr<TFakeMetricRegistry>& registry,
    const std::string& dbNamespace,
    const std::string& name,
    const std::string& operation)
{
    return registry->GetCounter(name, {
        {"db.system.name", "ydb"},
        {"db.namespace", dbNamespace},
        {"db.operation.name", operation},
        {"ydb.client.api", "Query"},
    });
}

std::shared_ptr<TFakeHistogram> GetDuration(
    const std::shared_ptr<TFakeMetricRegistry>& registry,
    const std::string& dbNamespace,
    const std::string& operation,
    EStatus status)
{
    NMetrics::TLabels labels = {
        {"db.system.name", "ydb"},
        {"db.namespace", dbNamespace},
        {"db.operation.name", operation},
        {"ydb.client.api", "Query"},
        {"db.response.status_code", ToString(status)},
    };
    if (status != EStatus::SUCCESS) {
        labels["error.type"] = ToString(status);
    }
    return registry->GetHistogram("db.client.operation.duration", labels);
}

void SkipQueryMetricsIntegrationIfNoEnv() {
    if (GetEnvOrEmpty("YDB_ENDPOINT").empty() || GetEnvOrEmpty("YDB_DATABASE").empty()) {
        GTEST_SKIP() << "Set YDB_ENDPOINT and YDB_DATABASE to run QueryMetricsIntegration tests";
    }
}

} // namespace

TEST(QueryMetricsIntegration, ExecuteQuerySuccessRecordsMetrics) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto [driver, registry, database] = MakeRunArgs();
    TQueryClient client(driver, TClientSettings().Database(database));

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto result = session.GetSession().ExecuteQuery(
        "SELECT 1;",
        TTxControl::BeginTx().CommitTx()
    ).ExtractValueSync();
    ASSERT_EQ(result.GetStatus(), EStatus::SUCCESS) << result.GetIssues().ToString();

    auto requests = GetCounter(registry, database, "db.client.operation.requests", "ExecuteQuery");
    ASSERT_NE(requests, nullptr) << "ExecuteQuery request counter not created";
    EXPECT_GE(requests->Get(), 1);

    auto errors = GetCounter(registry, database, "db.client.operation.errors", "ExecuteQuery");
    ASSERT_NE(errors, nullptr);
    EXPECT_EQ(errors->Get(), 0);

    auto duration = GetDuration(registry, database, "ExecuteQuery", EStatus::SUCCESS);
    ASSERT_NE(duration, nullptr) << "ExecuteQuery duration histogram not created";
    EXPECT_GE(duration->Count(), 1u);
    for (double v : duration->GetValues()) {
        EXPECT_GE(v, 0.0);
    }

    driver.Stop(true);
}

TEST(QueryMetricsIntegration, ExecuteQueryErrorRecordsErrorMetric) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto [driver, registry, database] = MakeRunArgs();
    TQueryClient client(driver, TClientSettings().Database(database));

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto result = session.GetSession().ExecuteQuery(
        "INVALID SQL QUERY !!!",
        TTxControl::BeginTx().CommitTx()
    ).ExtractValueSync();
    EXPECT_NE(result.GetStatus(), EStatus::SUCCESS);

    auto requests = GetCounter(registry, database, "db.client.operation.requests", "ExecuteQuery");
    ASSERT_NE(requests, nullptr);
    EXPECT_GE(requests->Get(), 1);

    auto errors = GetCounter(registry, database, "db.client.operation.errors", "ExecuteQuery");
    ASSERT_NE(errors, nullptr);
    EXPECT_GE(errors->Get(), 1);

    auto duration = GetDuration(registry, database, "ExecuteQuery", result.GetStatus());
    ASSERT_NE(duration, nullptr);
    EXPECT_GE(duration->Count(), 1u);

    driver.Stop(true);
}

TEST(QueryMetricsIntegration, CreateSessionRecordsMetrics) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto [driver, registry, database] = MakeRunArgs();
    TQueryClient client(driver, TClientSettings().Database(database));

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto requests = GetCounter(registry, database, "db.client.operation.requests", "GetSession");
    ASSERT_NE(requests, nullptr) << "CreateSession request counter not created";
    EXPECT_GE(requests->Get(), 1);

    auto duration = GetDuration(registry, database, "GetSession", EStatus::SUCCESS);
    ASSERT_NE(duration, nullptr) << "CreateSession duration histogram not created";
    EXPECT_GE(duration->Count(), 1u);

    driver.Stop(true);
}

TEST(QueryMetricsIntegration, CommitTransactionRecordsMetrics) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto [driver, registry, database] = MakeRunArgs();
    TQueryClient client(driver, TClientSettings().Database(database));

    auto sessionResult = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResult.IsSuccess()) << sessionResult.GetIssues().ToString();
    auto session = sessionResult.GetSession();

    auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
    ASSERT_TRUE(beginResult.IsSuccess()) << beginResult.GetIssues().ToString();
    auto tx = beginResult.GetTransaction();

    auto execResult = tx.GetSession().ExecuteQuery(
        "SELECT 1;",
        TTxControl::Tx(tx)
    ).ExtractValueSync();
    ASSERT_EQ(execResult.GetStatus(), EStatus::SUCCESS) << execResult.GetIssues().ToString();

    if (execResult.GetTransaction()) {
        auto commitResult = execResult.GetTransaction()->Commit().ExtractValueSync();
        ASSERT_TRUE(commitResult.IsSuccess()) << commitResult.GetIssues().ToString();

        auto commitRequests = GetCounter(registry, database, "db.client.operation.requests", "Commit");
        ASSERT_NE(commitRequests, nullptr) << "Commit request counter not created";
        EXPECT_GE(commitRequests->Get(), 1);

        auto commitDuration = GetDuration(registry, database, "Commit", EStatus::SUCCESS);
        ASSERT_NE(commitDuration, nullptr);
        EXPECT_GE(commitDuration->Count(), 1u);
    }

    driver.Stop(true);
}

TEST(QueryMetricsIntegration, RollbackTransactionRecordsMetrics) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto [driver, registry, database] = MakeRunArgs();
    TQueryClient client(driver, TClientSettings().Database(database));

    auto sessionResult = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResult.IsSuccess()) << sessionResult.GetIssues().ToString();
    auto session = sessionResult.GetSession();

    auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
    ASSERT_TRUE(beginResult.IsSuccess()) << beginResult.GetIssues().ToString();
    auto tx = beginResult.GetTransaction();

    auto rollbackResult = tx.Rollback().ExtractValueSync();
    ASSERT_TRUE(rollbackResult.IsSuccess()) << rollbackResult.GetIssues().ToString();

    auto rollbackRequests = GetCounter(registry, database, "db.client.operation.requests", "Rollback");
    ASSERT_NE(rollbackRequests, nullptr) << "Rollback request counter not created";
    EXPECT_GE(rollbackRequests->Get(), 1);

    auto rollbackErrors = GetCounter(registry, database, "db.client.operation.errors", "Rollback");
    ASSERT_NE(rollbackErrors, nullptr);
    EXPECT_EQ(rollbackErrors->Get(), 0);

    auto rollbackDuration = GetDuration(registry, database, "Rollback", EStatus::SUCCESS);
    ASSERT_NE(rollbackDuration, nullptr);
    EXPECT_GE(rollbackDuration->Count(), 1u);

    driver.Stop(true);
}

TEST(QueryMetricsIntegration, MultipleQueriesAccumulateMetrics) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto [driver, registry, database] = MakeRunArgs();
    TQueryClient client(driver, TClientSettings().Database(database));

    auto sessionResult = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResult.IsSuccess()) << sessionResult.GetIssues().ToString();
    auto session = sessionResult.GetSession();

    const int numQueries = 5;
    for (int i = 0; i < numQueries; ++i) {
        auto result = session.ExecuteQuery(
            "SELECT 1;",
            TTxControl::BeginTx().CommitTx()
        ).ExtractValueSync();
        ASSERT_EQ(result.GetStatus(), EStatus::SUCCESS) << result.GetIssues().ToString();
    }

    auto requests = GetCounter(registry, database, "db.client.operation.requests", "ExecuteQuery");
    ASSERT_NE(requests, nullptr);
    EXPECT_EQ(requests->Get(), numQueries);

    auto errors = GetCounter(registry, database, "db.client.operation.errors", "ExecuteQuery");
    ASSERT_NE(errors, nullptr);
    EXPECT_EQ(errors->Get(), 0);

    auto duration = GetDuration(registry, database, "ExecuteQuery", EStatus::SUCCESS);
    ASSERT_NE(duration, nullptr);
    EXPECT_EQ(duration->Count(), static_cast<size_t>(numQueries));

    driver.Stop(true);
}

TEST(QueryMetricsIntegration, NoRegistryDoesNotBreakOperations) {
    SkipQueryMetricsIntegrationIfNoEnv();
    std::string endpoint = GetEnvOrEmpty("YDB_ENDPOINT");
    std::string database = GetEnvOrEmpty("YDB_DATABASE");

    auto driverConfig = TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(GetEnvOrEmpty("YDB_TOKEN"));

    TDriver driver(driverConfig);
    TQueryClient client(driver, TClientSettings().Database(database));

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto result = session.GetSession().ExecuteQuery(
        "SELECT 1;",
        TTxControl::BeginTx().CommitTx()
    ).ExtractValueSync();
    EXPECT_EQ(result.GetStatus(), EStatus::SUCCESS) << result.GetIssues().ToString();

    driver.Stop(true);
}

TEST(QueryMetricsIntegration, DurationValuesAreRealistic) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto [driver, registry, database] = MakeRunArgs();
    TQueryClient client(driver, TClientSettings().Database(database));

    auto sessionResult = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResult.IsSuccess()) << sessionResult.GetIssues().ToString();
    auto session = sessionResult.GetSession();

    auto result = session.ExecuteQuery(
        "SELECT 1;",
        TTxControl::BeginTx().CommitTx()
    ).ExtractValueSync();
    ASSERT_EQ(result.GetStatus(), EStatus::SUCCESS) << result.GetIssues().ToString();

    auto duration = GetDuration(registry, database, "ExecuteQuery", EStatus::SUCCESS);
    ASSERT_NE(duration, nullptr);
    ASSERT_GE(duration->Count(), 1u);

    for (double v : duration->GetValues()) {
        EXPECT_GE(v, 0.0) << "Duration must be non-negative";
        EXPECT_LT(v, 30.0) << "Duration > 30s is unrealistic for SELECT 1";
    }

    driver.Stop(true);
}

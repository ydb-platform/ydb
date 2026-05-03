#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/stats/stats.h>
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
    std::string ServerAddress;
    std::uint16_t ServerPort = 0;
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

    std::string host;
    std::uint16_t port = 0;
    NSdkStats::TStatCollector::ParseDiscoveryEndpoint(endpoint, host, port);

    return {std::move(driver), registry, std::move(database), std::move(host), port};
}

NMetrics::TLabels FailedLabels(const std::string& dbNamespace,
                               const std::string& operation) {
    return {
        {"db.system.name", "ydb"},
        {"db.namespace", dbNamespace},
        {"db.operation.name", operation},
    };
}

NMetrics::TLabels DurationLabels(const std::string& dbNamespace,
                                 const std::string& operation,
                                 const std::string& serverAddress,
                                 std::uint16_t serverPort) {
    NMetrics::TLabels labels = {
        {"db.system.name", "ydb"},
        {"db.namespace", dbNamespace},
        {"db.operation.name", operation},
    };
    if (!serverAddress.empty()) {
        labels["server.address"] = serverAddress;
    }
    if (serverPort != 0) {
        labels["server.port"] = ToString(serverPort);
    }
    return labels;
}

NMetrics::TLabels PoolLabels(const std::string& dbNamespace,
                             const std::string& clientApi) {
    return {
        {"db.system.name", "ydb"},
        {"db.namespace", dbNamespace},
        {"db.client.connection.pool.name", clientApi},
    };
}

NMetrics::TLabels CountLabels(const std::string& dbNamespace,
                              const std::string& clientApi,
                              const std::string& state) {
    auto labels = PoolLabels(dbNamespace, clientApi);
    labels["db.client.connection.state"] = state;
    return labels;
}

void SkipQueryMetricsIntegrationIfNoEnv() {
    if (GetEnvOrEmpty("YDB_ENDPOINT").empty() || GetEnvOrEmpty("YDB_DATABASE").empty()) {
        GTEST_SKIP() << "Set YDB_ENDPOINT and YDB_DATABASE to run QueryMetricsIntegration tests";
    }
}

} // namespace

// ---------------------------------------------------------------------------
// db.client.operation.duration / db.client.operation.failed
// ---------------------------------------------------------------------------

TEST(QueryMetricsIntegration, ExecuteQuerySuccessRecordsDurationOnly) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto result = session.GetSession().ExecuteQuery(
        "SELECT 1;",
        TTxControl::BeginTx().CommitTx()
    ).ExtractValueSync();
    ASSERT_EQ(result.GetStatus(), EStatus::SUCCESS) << result.GetIssues().ToString();

    auto duration = args.Registry->GetHistogram(
        "db.client.operation.duration",
        DurationLabels(args.Database, "ydb.ExecuteQuery", args.ServerAddress, args.ServerPort));
    ASSERT_NE(duration, nullptr) << "ExecuteQuery duration histogram not created";
    EXPECT_GE(duration->Count(), 1u);
    for (double v : duration->GetValues()) {
        EXPECT_GE(v, 0.0);
    }

    auto failed = args.Registry->GetCounter(
        "db.client.operation.failed",
        FailedLabels(args.Database, "ydb.ExecuteQuery"));
    if (failed) {
        EXPECT_EQ(failed->Get(), 0);
    }

    args.Driver.Stop(true);
}

TEST(QueryMetricsIntegration, ExecuteQueryErrorIncrementsFailed) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto result = session.GetSession().ExecuteQuery(
        "INVALID SQL QUERY !!!",
        TTxControl::BeginTx().CommitTx()
    ).ExtractValueSync();
    EXPECT_NE(result.GetStatus(), EStatus::SUCCESS);

    auto failed = args.Registry->GetCounter(
        "db.client.operation.failed",
        FailedLabels(args.Database, "ydb.ExecuteQuery"));
    ASSERT_NE(failed, nullptr);
    EXPECT_GE(failed->Get(), 1);

    auto duration = args.Registry->GetHistogram(
        "db.client.operation.duration",
        DurationLabels(args.Database, "ydb.ExecuteQuery", args.ServerAddress, args.ServerPort));
    ASSERT_NE(duration, nullptr);
    EXPECT_GE(duration->Count(), 1u);

    args.Driver.Stop(true);
}

TEST(QueryMetricsIntegration, CreateSessionRecordsDuration) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto duration = args.Registry->GetHistogram(
        "db.client.operation.duration",
        DurationLabels(args.Database, "ydb.CreateSession", args.ServerAddress, args.ServerPort));
    ASSERT_NE(duration, nullptr) << "CreateSession duration histogram not created";
    EXPECT_GE(duration->Count(), 1u);

    args.Driver.Stop(true);
}

TEST(QueryMetricsIntegration, CommitTransactionRecordsDuration) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

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

        auto commitDuration = args.Registry->GetHistogram(
            "db.client.operation.duration",
            DurationLabels(args.Database, "ydb.Commit", args.ServerAddress, args.ServerPort));
        ASSERT_NE(commitDuration, nullptr);
        EXPECT_GE(commitDuration->Count(), 1u);
    }

    args.Driver.Stop(true);
}

TEST(QueryMetricsIntegration, BeginTransactionRecordsDuration) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto sessionResult = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResult.IsSuccess()) << sessionResult.GetIssues().ToString();
    auto session = sessionResult.GetSession();

    auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
    ASSERT_TRUE(beginResult.IsSuccess()) << beginResult.GetIssues().ToString();

    auto beginDuration = args.Registry->GetHistogram(
        "db.client.operation.duration",
        DurationLabels(args.Database, "ydb.BeginTransaction", args.ServerAddress, args.ServerPort));
    ASSERT_NE(beginDuration, nullptr) << "BeginTransaction duration histogram not created";
    EXPECT_GE(beginDuration->Count(), 1u);

    auto beginFailed = args.Registry->GetCounter(
        "db.client.operation.failed",
        FailedLabels(args.Database, "ydb.BeginTransaction"));
    if (beginFailed) {
        EXPECT_EQ(beginFailed->Get(), 0);
    }

    args.Driver.Stop(true);
}

TEST(QueryMetricsIntegration, RollbackTransactionRecordsDuration) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto sessionResult = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResult.IsSuccess()) << sessionResult.GetIssues().ToString();
    auto session = sessionResult.GetSession();

    auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
    ASSERT_TRUE(beginResult.IsSuccess()) << beginResult.GetIssues().ToString();
    auto tx = beginResult.GetTransaction();

    auto rollbackResult = tx.Rollback().ExtractValueSync();
    ASSERT_TRUE(rollbackResult.IsSuccess()) << rollbackResult.GetIssues().ToString();

    auto rollbackDuration = args.Registry->GetHistogram(
        "db.client.operation.duration",
        DurationLabels(args.Database, "ydb.Rollback", args.ServerAddress, args.ServerPort));
    ASSERT_NE(rollbackDuration, nullptr);
    EXPECT_GE(rollbackDuration->Count(), 1u);

    auto rollbackFailed = args.Registry->GetCounter(
        "db.client.operation.failed",
        FailedLabels(args.Database, "ydb.Rollback"));
    if (rollbackFailed) {
        EXPECT_EQ(rollbackFailed->Get(), 0);
    }

    args.Driver.Stop(true);
}

TEST(QueryMetricsIntegration, MultipleQueriesAccumulateDuration) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

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

    auto duration = args.Registry->GetHistogram(
        "db.client.operation.duration",
        DurationLabels(args.Database, "ydb.ExecuteQuery", args.ServerAddress, args.ServerPort));
    ASSERT_NE(duration, nullptr);
    EXPECT_GE(duration->Count(), static_cast<size_t>(numQueries));

    auto failed = args.Registry->GetCounter(
        "db.client.operation.failed",
        FailedLabels(args.Database, "ydb.ExecuteQuery"));
    if (failed) {
        EXPECT_EQ(failed->Get(), 0);
    }

    args.Driver.Stop(true);
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
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto sessionResult = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResult.IsSuccess()) << sessionResult.GetIssues().ToString();
    auto session = sessionResult.GetSession();

    auto result = session.ExecuteQuery(
        "SELECT 1;",
        TTxControl::BeginTx().CommitTx()
    ).ExtractValueSync();
    ASSERT_EQ(result.GetStatus(), EStatus::SUCCESS) << result.GetIssues().ToString();

    auto duration = args.Registry->GetHistogram(
        "db.client.operation.duration",
        DurationLabels(args.Database, "ydb.ExecuteQuery", args.ServerAddress, args.ServerPort));
    ASSERT_NE(duration, nullptr);
    ASSERT_GE(duration->Count(), 1u);

    for (double v : duration->GetValues()) {
        EXPECT_GE(v, 0.0) << "Duration must be non-negative";
        EXPECT_LT(v, 30.0) << "Duration > 30s is unrealistic for SELECT 1";
    }

    args.Driver.Stop(true);
}

// ---------------------------------------------------------------------------
// db.client.connection.* (pool metrics)
// ---------------------------------------------------------------------------

TEST(QueryMetricsIntegration, ConnectionCreateTimeRecorded) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto hist = args.Registry->GetHistogram(
        "db.client.connection.create_time",
        PoolLabels(args.Database, "Query"));
    ASSERT_NE(hist, nullptr) << "create_time histogram was not created";
    EXPECT_GE(hist->Count(), 1u);
    for (double v : hist->GetValues()) {
        EXPECT_GE(v, 0.0);
    }

    args.Driver.Stop(true);
}

TEST(QueryMetricsIntegration, ConnectionCountIsSplitByState) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto sessionResult = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResult.IsSuccess()) << sessionResult.GetIssues().ToString();

    auto used = args.Registry->GetGauge(
        "db.client.connection.count",
        CountLabels(args.Database, "Query", "used"));
    ASSERT_NE(used, nullptr) << "connection.count gauge with state=used not created";
    EXPECT_GE(used->Get(), 1.0);

    auto idle = args.Registry->GetGauge(
        "db.client.connection.count",
        CountLabels(args.Database, "Query", "idle"));
    ASSERT_NE(idle, nullptr) << "connection.count gauge with state=idle not created";

    args.Driver.Stop(true);
}

TEST(QueryMetricsIntegration, ConnectionPendingRequestsGaugeExists) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto gauge = args.Registry->GetGauge(
        "db.client.connection.pending_requests",
        PoolLabels(args.Database, "Query"));
    ASSERT_NE(gauge, nullptr) << "pending_requests gauge not created";

    args.Driver.Stop(true);
}

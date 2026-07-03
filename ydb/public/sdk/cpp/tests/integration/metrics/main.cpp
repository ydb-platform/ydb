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
    std::string Endpoint;
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

    return {std::move(driver), registry, std::move(database), endpoint, std::move(host), port};
}

NMetrics::TLabels FailedLabels(const std::string& dbNamespace
    , const std::string& operation
    , EStatus status
    , const std::string& serverAddress
    , std::uint16_t serverPort
) {
    NMetrics::TLabels labels = {
        {"db.system.name", "ydb"},
        {"db.namespace", dbNamespace},
        {"db.operation.name", operation},
        {"db.response.status_code", ToString(status)},
    };
    if (!serverAddress.empty()) {
        labels["server.address"] = serverAddress;
    }
    if (serverPort != 0) {
        labels["server.port"] = ToString(serverPort);
    }
    return labels;
}

NMetrics::TLabels DurationLabels(const std::string& dbNamespace
    , const std::string& operation
    , const std::string& serverAddress
    , std::uint16_t serverPort
) {
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

NMetrics::TLabels QueryPoolLabels(
    const std::string& database
    , const std::string& endpoint
) {
    return {
        {"ydb.query.session.pool.name", database + "@" + endpoint},
    };
}

NMetrics::TLabels QueryCountLabels(
    const std::string& database
    , const std::string& endpoint
    , const std::string& state
) {
    auto labels = QueryPoolLabels(database, endpoint);
    labels["ydb.query.session.state"] = state;
    return labels;
}

void SkipQueryMetricsIntegrationIfNoEnv() {
    if (GetEnvOrEmpty("YDB_ENDPOINT").empty() || GetEnvOrEmpty("YDB_DATABASE").empty()) {
        GTEST_SKIP() << "Set YDB_ENDPOINT and YDB_DATABASE to run QueryMetricsIntegration tests";
    }
}

} // namespace

// ---------------------------------------------------------------------------
// ydb.client.operation.duration / ydb.client.operation.failed
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
        "ydb.client.operation.duration",
        DurationLabels(args.Database, "ExecuteQuery", args.ServerAddress, args.ServerPort)
    );
    ASSERT_NE(duration, nullptr) << "ExecuteQuery duration histogram not created";
    EXPECT_GE(duration->Count(), 1u);
    for (double v : duration->GetValues()) {
        EXPECT_GE(v, 0.0);
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
        "ydb.client.operation.failed",
        FailedLabels(
            args.Database,
            "ExecuteQuery",
            result.GetStatus(),
            args.ServerAddress,
            args.ServerPort
        )
    );
    ASSERT_NE(failed, nullptr);
    EXPECT_GE(failed->Get(), 1);

    auto duration = args.Registry->GetHistogram(
        "ydb.client.operation.duration",
        DurationLabels(args.Database, "ExecuteQuery", args.ServerAddress, args.ServerPort));
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
        "ydb.client.operation.duration",
        DurationLabels(args.Database, "CreateSession", args.ServerAddress, args.ServerPort));
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
            "ydb.client.operation.duration",
            DurationLabels(args.Database, "Commit", args.ServerAddress, args.ServerPort)
        );
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
        "ydb.client.operation.duration",
        DurationLabels(args.Database, "BeginTransaction", args.ServerAddress, args.ServerPort)
    );
    ASSERT_NE(beginDuration, nullptr) << "BeginTransaction duration histogram not created";
    EXPECT_GE(beginDuration->Count(), 1u);

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
        "ydb.client.operation.duration",
        DurationLabels(args.Database, "Rollback", args.ServerAddress, args.ServerPort)
    );
    ASSERT_NE(rollbackDuration, nullptr);
    EXPECT_GE(rollbackDuration->Count(), 1u);

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
        "ydb.client.operation.duration",
        DurationLabels(args.Database, "ExecuteQuery", args.ServerAddress, args.ServerPort)
    );
    ASSERT_NE(duration, nullptr);
    EXPECT_GE(duration->Count(), static_cast<size_t>(numQueries));

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
        "ydb.client.operation.duration",
        DurationLabels(args.Database, "ExecuteQuery", args.ServerAddress, args.ServerPort)
    );
    ASSERT_NE(duration, nullptr);
    ASSERT_GE(duration->Count(), 1u);

    for (double v : duration->GetValues()) {
        EXPECT_GE(v, 0.0) << "Duration must be non-negative";
        EXPECT_LT(v, 30.0) << "Duration > 30s is unrealistic for SELECT 1";
    }

    args.Driver.Stop(true);
}

// ---------------------------------------------------------------------------
// ydb.query.session.* (pool metrics)
// ---------------------------------------------------------------------------

TEST(QueryMetricsIntegration, SessionCreateTimeRecorded) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto hist = args.Registry->GetHistogram(
        "ydb.query.session.create_time",
        QueryPoolLabels(args.Database, args.Endpoint)
    );
    ASSERT_NE(hist, nullptr) << "ydb.query.session.create_time histogram was not created";
    EXPECT_GE(hist->Count(), 1u);
    for (double v : hist->GetValues()) {
        EXPECT_GE(v, 0.0);
    }

    args.Driver.Stop(true);
}

TEST(QueryMetricsIntegration, SessionCountIsSplitByState) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    TQueryClient client(args.Driver, TClientSettings().Database(args.Database));

    auto sessionResult = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResult.IsSuccess()) << sessionResult.GetIssues().ToString();

    auto used = args.Registry->GetGauge(
        "ydb.query.session.count",
        QueryCountLabels(args.Database, args.Endpoint, "used")
    );
    ASSERT_NE(used, nullptr) << "ydb.query.session.count gauge with state=used not created";
    EXPECT_GE(used->Get(), 1.0);

    auto idle = args.Registry->GetGauge(
        "ydb.query.session.count",
        QueryCountLabels(args.Database, args.Endpoint, "idle"));
    ASSERT_NE(idle, nullptr) << "ydb.query.session.count gauge with state=idle not created";

    args.Driver.Stop(true);
}

TEST(QueryMetricsIntegration, MaxAndMinSessionGaugesAreEmitted) {
    SkipQueryMetricsIntegrationIfNoEnv();
    auto args = MakeRunArgs();
    auto settings = TClientSettings()
        .Database(args.Database)
        .SessionPoolSettings(TSessionPoolSettings()
            .MaxActiveSessions(50)
            .MinPoolSize(10)
        );
    TQueryClient client(args.Driver, settings);

    auto session = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(session.IsSuccess()) << session.GetIssues().ToString();

    auto maxGauge = args.Registry->GetGauge(
        "ydb.query.session.max",
        QueryPoolLabels(args.Database, args.Endpoint)
    );
    auto minGauge = args.Registry->GetGauge(
        "ydb.query.session.min",
        QueryPoolLabels(args.Database, args.Endpoint)
    );
    ASSERT_NE(maxGauge, nullptr) << "ydb.query.session.max gauge not created";
    ASSERT_NE(minGauge, nullptr) << "ydb.query.session.min gauge not created";
    EXPECT_DOUBLE_EQ(maxGauge->Get(), 50.0);
    EXPECT_DOUBLE_EQ(minGauge->Get(), 10.0);

    args.Driver.Stop(true);
}

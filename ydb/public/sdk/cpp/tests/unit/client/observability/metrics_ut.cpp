#include <ydb/public/sdk/cpp/src/client/impl/observability/metrics.h>
#include <ydb/public/sdk/cpp/src/client/impl/stats/stats.h>
#include <ydb/public/sdk/cpp/tests/common/fake_metric_registry.h>
#include <util/string/cast.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NObservability;
using namespace NYdb::NMetrics;
using namespace NYdb::NTests;
using namespace NYdb::NSdkStats;

namespace {
    constexpr const char kTestDbNamespace[] = "/Root/testdb";

    std::string YdbOp(const std::string& op) {
        return op.rfind("ydb.", 0) == 0 ? op : "ydb." + op;
    }
} // namespace

// ---------------------------------------------------------------------------
// TRequestMetrics (shared logic)
// ---------------------------------------------------------------------------

class RequestMetricsTest : public ::testing::Test {
protected:
    void SetUp() override {
        Registry = std::make_shared<TFakeMetricRegistry>();
        OpCollector = TStatCollector::TClientOperationStatCollector(
            nullptr, kTestDbNamespace, "", Registry);
    }

    std::shared_ptr<TFakeCounter> FailedCounter(const std::string& op, EStatus status) {
        const std::string statusName = ToString(status);
        return Registry->GetCounter("db.client.operation.failed", {
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", YdbOp(op)},
            {"ydb.client.api", "Unspecified"},
            {"db.response.status_code", statusName},
            {"error.type", statusName},
        });
    }

    std::shared_ptr<TFakeHistogram> DurationHistogram(const std::string& op, EStatus status) {
        TLabels labels = {
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", YdbOp(op)},
            {"ydb.client.api", "Unspecified"},
        };
        if (status != EStatus::SUCCESS) {
            labels["db.response.status_code"] = ToString(status);
            labels["error.type"] = ToString(status);
        }
        return Registry->GetHistogram("db.client.operation.duration", labels);
    }

    TStatCollector::TClientOperationStatCollector OpCollector;
    std::shared_ptr<TFakeMetricRegistry> Registry;
};

TEST_F(RequestMetricsTest, SuccessDoesNotIncrementFailedCounter) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::SUCCESS);
    }

    auto failed = FailedCounter("DoSomething", EStatus::UNAVAILABLE);
    if (failed) {
        EXPECT_EQ(failed->Get(), 0);
    }
}

TEST_F(RequestMetricsTest, FailureIncrementsFailedCounter) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::UNAVAILABLE);
    }

    auto failed = FailedCounter("DoSomething", EStatus::UNAVAILABLE);
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->Get(), 1);
}

TEST_F(RequestMetricsTest, DurationRecordedOnEnd) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::SUCCESS);
    }

    auto hist = DurationHistogram("DoSomething", EStatus::SUCCESS);
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->Count(), 1u);
    EXPECT_GE(hist->GetValues()[0], 0.0);
}

TEST_F(RequestMetricsTest, DurationIsInSeconds) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::SUCCESS);
    }

    auto hist = DurationHistogram("DoSomething", EStatus::SUCCESS);
    ASSERT_NE(hist, nullptr);
    EXPECT_LT(hist->GetValues()[0], 1.0);
}

TEST_F(RequestMetricsTest, DoubleEndIsIdempotent) {
    TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
    metrics.End(EStatus::SUCCESS);
    metrics.End(EStatus::INTERNAL_ERROR);

    auto failed = FailedCounter("DoSomething", EStatus::INTERNAL_ERROR);
    if (failed) {
        EXPECT_EQ(failed->Get(), 0);
    }

    auto hist = DurationHistogram("DoSomething", EStatus::SUCCESS);
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->Count(), 1u);
}

TEST_F(RequestMetricsTest, DestructorCallsEndWithClientInternalError) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
    }

    auto failed = FailedCounter("DoSomething", EStatus::CLIENT_INTERNAL_ERROR);
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->Get(), 1);

    auto hist = DurationHistogram("DoSomething", EStatus::CLIENT_INTERNAL_ERROR);
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->Count(), 1u);
}

TEST_F(RequestMetricsTest, NullRegistryDoesNotCrash) {
    EXPECT_NO_THROW({
        TStatCollector::TClientOperationStatCollector nullCollector;
        TRequestMetrics metrics(&nullCollector, "DoSomething", TLog());
        metrics.End(EStatus::SUCCESS);
    });
}

TEST_F(RequestMetricsTest, DifferentOperationsHaveSeparateMetrics) {
    {
        TRequestMetrics m1(&OpCollector, "OpA", TLog());
        m1.End(EStatus::SUCCESS);
    }
    {
        TRequestMetrics m2(&OpCollector, "OpB", TLog());
        m2.End(EStatus::OVERLOADED);
    }

    auto failedA = FailedCounter("OpA", EStatus::SUCCESS);
    if (failedA) {
        EXPECT_EQ(failedA->Get(), 0);
    }
    auto failedB = FailedCounter("OpB", EStatus::OVERLOADED);
    ASSERT_NE(failedB, nullptr);
    EXPECT_EQ(failedB->Get(), 1);
    EXPECT_EQ(DurationHistogram("OpA", EStatus::SUCCESS)->Count(), 1u);
    EXPECT_EQ(DurationHistogram("OpB", EStatus::OVERLOADED)->Count(), 1u);
}

TEST_F(RequestMetricsTest, MultipleRequestsAccumulate) {
    for (int i = 0; i < 5; ++i) {
        TRequestMetrics metrics(&OpCollector, "Op", TLog());
        metrics.End(i % 2 == 0 ? EStatus::SUCCESS : EStatus::TIMEOUT);
    }

    auto failed = FailedCounter("Op", EStatus::TIMEOUT);
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->Get(), 2);
    EXPECT_EQ(DurationHistogram("Op", EStatus::SUCCESS)->Count(), 3u);
    EXPECT_EQ(DurationHistogram("Op", EStatus::TIMEOUT)->Count(), 2u);
}

TEST_F(RequestMetricsTest, AllErrorStatusesIncrementFailedCounter) {
    std::vector<EStatus> errorStatuses = {
        EStatus::BAD_REQUEST,
        EStatus::UNAUTHORIZED,
        EStatus::INTERNAL_ERROR,
        EStatus::UNAVAILABLE,
        EStatus::OVERLOADED,
        EStatus::TIMEOUT,
        EStatus::NOT_FOUND,
        EStatus::CLIENT_INTERNAL_ERROR,
    };

    for (auto status : errorStatuses) {
        TRequestMetrics metrics(&OpCollector, "Op", TLog());
        metrics.End(status);
    }

    for (auto status : errorStatuses) {
        auto failed = FailedCounter("Op", status);
        ASSERT_NE(failed, nullptr);
        EXPECT_EQ(failed->Get(), 1) << "status " << ToString(status);
    }
}

TEST_F(RequestMetricsTest, DeprecatedRequestAndErrorCountersAreNotEmitted) {
    {
        TRequestMetrics metrics(&OpCollector, "Op", TLog());
        metrics.End(EStatus::UNAVAILABLE);
    }

    TLabels baseLabels = {
        {"db.system.name", "ydb"},
        {"db.namespace", kTestDbNamespace},
        {"db.operation.name", "ydb.Op"},
        {"ydb.client.api", "Unspecified"},
    };
    EXPECT_EQ(Registry->GetCounter("db.client.operation.requests", baseLabels), nullptr);
    EXPECT_EQ(Registry->GetCounter("db.client.operation.errors", baseLabels), nullptr);
}

TEST(RequestMetricsDbNamespaceTest, DifferentNamespacesAreSeparateMetricSeries) {
    auto registry = std::make_shared<TFakeMetricRegistry>();
    TStatCollector::TClientOperationStatCollector collectorA(nullptr, "/db/alpha", "", registry);
    TStatCollector::TClientOperationStatCollector collectorB(nullptr, "/db/beta", "", registry);

    {
        TRequestMetrics m(&collectorA, "GetSession", TLog());
        m.End(EStatus::SUCCESS);
    }
    {
        TRequestMetrics m(&collectorB, "GetSession", TLog());
        m.End(EStatus::SUCCESS);
    }

    auto labelsAlpha = NMetrics::TLabels{
        {"db.system.name", "ydb"},
        {"db.namespace", "/db/alpha"},
        {"db.operation.name", "ydb.GetSession"},
        {"ydb.client.api", "Unspecified"},
    };
    auto labelsBeta = NMetrics::TLabels{
        {"db.system.name", "ydb"},
        {"db.namespace", "/db/beta"},
        {"db.operation.name", "ydb.GetSession"},
        {"ydb.client.api", "Unspecified"},
    };

    auto reqAlpha = registry->GetCounter("db.client.operation.requests", labelsAlpha("ydb.GetSession"));
    auto reqBeta = registry->GetCounter("db.client.operation.requests", labelsBeta("ydb.GetSession"));
    ASSERT_NE(reqAlpha, nullptr);
    ASSERT_NE(reqBeta, nullptr);
    EXPECT_EQ(reqAlpha->Get(), 1);
    EXPECT_EQ(reqBeta->Get(), 1);

    auto durLabels = [&](auto base, const char* op) {
        auto labels = base(op);
        labels["db.response.status_code"] = ToString(EStatus::SUCCESS);
        return labels;
    };
    auto durAlpha = registry->GetHistogram(
        "db.client.operation.duration",
        durLabels(labelsAlpha, "ydb.GetSession"));
    auto durBeta = registry->GetHistogram(
        "db.client.operation.duration",
        durLabels(labelsBeta, "ydb.GetSession"));
    ASSERT_NE(durAlpha, nullptr);
    ASSERT_NE(durBeta, nullptr);
    EXPECT_EQ(durAlpha->Count(), 1u);
    EXPECT_EQ(durBeta->Count(), 1u);
}

TEST(RequestMetricsClientAliasesTest, QueryOperationsUseOtelStandardMetrics) {
    auto registry = std::make_shared<TFakeMetricRegistry>();
    TStatCollector::TClientOperationStatCollector collector(nullptr, "", "Query", registry);

    NObservability::TRequestMetrics metrics(&collector, "ydb.ExecuteQuery", TLog());
    metrics.End(EStatus::SUCCESS);

    EXPECT_NE(
        registry->GetCounter(
            "db.client.operation.requests",
            {
                {"db.system.name", "ydb"},
                {"db.namespace", ""},
                {"db.operation.name", "ydb.ExecuteQuery"},
                {"ydb.client.api", "Query"},
            }
        ),
        nullptr
    );
    EXPECT_NE(
        registry->GetCounter(
            "db.client.operation.errors",
            {
                {"db.system.name", "ydb"},
                {"db.namespace", ""},
                {"db.operation.name", "ydb.ExecuteQuery"},
                {"ydb.client.api", "Query"},
            }
        ),
        nullptr
    );
    EXPECT_NE(
        registry->GetHistogram(
            "db.client.operation.duration",
            {
                {"db.system.name", "ydb"},
                {"db.namespace", ""},
                {"db.operation.name", "ydb.ExecuteQuery"},
                {"ydb.client.api", "Query"},
            }
        ),
        nullptr
    );
}

TEST(RequestMetricsClientAliasesTest, TableOperationsUseOtelStandardMetrics) {
    auto registry = std::make_shared<TFakeMetricRegistry>();
    TStatCollector::TClientOperationStatCollector collector(nullptr, "", "Table", registry);

    NObservability::TRequestMetrics metrics(&collector, "ExecuteDataQuery", TLog());
    metrics.End(EStatus::SUCCESS);

    EXPECT_NE(
        registry->GetHistogram(
            "db.client.operation.duration",
            {
                {"db.system.name", "ydb"},
                {"db.namespace", ""},
                {"db.operation.name", "ydb.ExecuteDataQuery"},
                {"ydb.client.api", "Table"},
            }
        ),
        nullptr
    );
}

// ---------------------------------------------------------------------------
// Session pool / connection metrics
// ---------------------------------------------------------------------------

namespace {
    NMetrics::TLabels PoolLabels(const std::string& database, const std::string& clientType) {
        return {
            {"db.system.name", "ydb"},
            {"db.namespace", database},
            {"db.client.connection.pool.name", clientType.empty() ? std::string("Unspecified") : clientType},
            {"ydb.client.api", clientType.empty() ? std::string("Unspecified") : clientType},
        };
    }
} // namespace

class ConnectionPoolMetricsTest : public ::testing::Test {
protected:
    void SetUp() override {
        Registry = std::make_shared<TFakeMetricRegistry>();
        Collector = TStatCollector::TSessionPoolStatCollector(
            /*activeSessions=*/nullptr,
            /*inPoolSessions=*/nullptr,
            /*fakeSessions=*/nullptr,
            /*waiters=*/nullptr,
            Registry,
            kTestDbNamespace,
            "Query");
    }

    std::shared_ptr<TFakeMetricRegistry> Registry;
    TStatCollector::TSessionPoolStatCollector Collector;
};

TEST_F(ConnectionPoolMetricsTest, CreateTimeRecorded) {
    Collector.RecordConnectionCreateTime(0.002);
    Collector.RecordConnectionCreateTime(0.100);

    auto hist = Registry->GetHistogram(
        "db.client.connection.create_time",
        PoolLabels(kTestDbNamespace, "Query"));
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->Count(), 2u);
    EXPECT_DOUBLE_EQ(hist->GetValues()[0], 0.002);
    EXPECT_DOUBLE_EQ(hist->GetValues()[1], 0.100);
}

TEST_F(ConnectionPoolMetricsTest, TimeoutsIncrement) {
    Collector.IncConnectionTimeouts();
    Collector.IncConnectionTimeouts();
    Collector.IncConnectionTimeouts();

    auto counter = Registry->GetCounter(
        "db.client.connection.timeouts",
        PoolLabels(kTestDbNamespace, "Query"));
    ASSERT_NE(counter, nullptr);
    EXPECT_EQ(counter->Get(), 3);
}

TEST_F(ConnectionPoolMetricsTest, ConnectionCountGauge) {
    Collector.UpdateConnectionCount(5);
    Collector.UpdateConnectionCount(10);
    Collector.UpdateConnectionCount(2);

    auto hist = Registry->GetHistogram(
        "db.client.connection.create_time",
        PoolLabels(kTestDbNamespace, "Query"));
    EXPECT_EQ(hist, nullptr); // was never recorded
}

TEST_F(ConnectionPoolMetricsTest, PendingRequestsGauge) {
    Collector.UpdatePendingRequests(0);
    Collector.UpdatePendingRequests(7);

    SUCCEED();
}

TEST(ConnectionPoolMetricsNoRegistryTest, NullRegistryIsSafe) {
    TStatCollector::TSessionPoolStatCollector collector;
    EXPECT_FALSE(collector.HasExternalRegistry());
    EXPECT_NO_THROW({
        collector.RecordConnectionCreateTime(1.0);
        collector.IncConnectionTimeouts();
        collector.UpdateConnectionCount(3);
        collector.UpdatePendingRequests(1);
    });
}

TEST(ConnectionPoolMetricsPoolNameTest, DifferentPoolsHaveSeparateMetrics) {
    auto registry = std::make_shared<TFakeMetricRegistry>();
    TStatCollector::TSessionPoolStatCollector queryPool(
        nullptr, nullptr, nullptr, nullptr, registry, kTestDbNamespace, "Query");
    TStatCollector::TSessionPoolStatCollector tablePool(
        nullptr, nullptr, nullptr, nullptr, registry, kTestDbNamespace, "Table");

    queryPool.IncConnectionTimeouts();
    tablePool.IncConnectionTimeouts();
    tablePool.IncConnectionTimeouts();

    auto queryCounter = registry->GetCounter(
        "db.client.connection.timeouts",
        PoolLabels(kTestDbNamespace, "Query"));
    auto tableCounter = registry->GetCounter(
        "db.client.connection.timeouts",
        PoolLabels(kTestDbNamespace, "Table"));

    ASSERT_NE(queryCounter, nullptr);
    ASSERT_NE(tableCounter, nullptr);
    EXPECT_EQ(queryCounter->Get(), 1);
    EXPECT_EQ(tableCounter->Get(), 2);
}

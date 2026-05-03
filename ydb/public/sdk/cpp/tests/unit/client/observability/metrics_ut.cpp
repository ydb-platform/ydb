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
    constexpr const char kTestServerAddress[] = "ydb.example.com";
    constexpr std::uint16_t kTestServerPort = 2135;

    std::string YdbOp(const std::string& op) {
        return op.rfind("ydb.", 0) == 0 ? op : "ydb." + op;
    }
} // namespace

// ---------------------------------------------------------------------------
// TRequestMetrics (db.client.operation.* — strict OTel-style label set)
// ---------------------------------------------------------------------------

class RequestMetricsTest : public ::testing::Test {
protected:
    void SetUp() override {
        Registry = std::make_shared<TFakeMetricRegistry>();
        OpCollector = TStatCollector::TClientOperationStatCollector(
            nullptr, kTestDbNamespace, "", Registry, kTestServerAddress, kTestServerPort);
    }

    static TLabels FailedLabels(const std::string& op) {
        return {
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", YdbOp(op)},
        };
    }

    static TLabels DurationLabels(const std::string& op) {
        return {
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", YdbOp(op)},
            {"server.address", kTestServerAddress},
            {"server.port", ToString(kTestServerPort)},
        };
    }

    std::shared_ptr<TFakeCounter> FailedCounter(const std::string& op) {
        return Registry->GetCounter("db.client.operation.failed", FailedLabels(op));
    }

    std::shared_ptr<TFakeHistogram> DurationHistogram(const std::string& op) {
        return Registry->GetHistogram("db.client.operation.duration", DurationLabels(op));
    }

    TStatCollector::TClientOperationStatCollector OpCollector;
    std::shared_ptr<TFakeMetricRegistry> Registry;
};

TEST_F(RequestMetricsTest, SuccessDoesNotIncrementFailedCounter) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::SUCCESS);
    }

    auto failed = FailedCounter("DoSomething");
    if (failed) {
        EXPECT_EQ(failed->Get(), 0);
    }
}

TEST_F(RequestMetricsTest, FailureIncrementsFailedCounter) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::UNAVAILABLE);
    }

    auto failed = FailedCounter("DoSomething");
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->Get(), 1);
}

TEST_F(RequestMetricsTest, DurationRecordedOnEnd) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::SUCCESS);
    }

    auto hist = DurationHistogram("DoSomething");
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->Count(), 1u);
    EXPECT_GE(hist->GetValues()[0], 0.0);
}

TEST_F(RequestMetricsTest, DurationIsInSeconds) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::SUCCESS);
    }

    auto hist = DurationHistogram("DoSomething");
    ASSERT_NE(hist, nullptr);
    EXPECT_LT(hist->GetValues()[0], 1.0);
}

TEST_F(RequestMetricsTest, DurationDoesNotSplitBySuccessOrError) {
    {
        TRequestMetrics m(&OpCollector, "Op", TLog());
        m.End(EStatus::SUCCESS);
    }
    {
        TRequestMetrics m(&OpCollector, "Op", TLog());
        m.End(EStatus::OVERLOADED);
    }

    auto hist = DurationHistogram("Op");
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->Count(), 2u);
}

TEST_F(RequestMetricsTest, DoubleEndIsIdempotent) {
    TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
    metrics.End(EStatus::SUCCESS);
    metrics.End(EStatus::INTERNAL_ERROR);

    auto failed = FailedCounter("DoSomething");
    if (failed) {
        EXPECT_EQ(failed->Get(), 0);
    }

    auto hist = DurationHistogram("DoSomething");
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->Count(), 1u);
}

TEST_F(RequestMetricsTest, DestructorCallsEndWithClientInternalError) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
    }

    auto failed = FailedCounter("DoSomething");
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->Get(), 1);

    auto hist = DurationHistogram("DoSomething");
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

    auto failedA = FailedCounter("OpA");
    if (failedA) {
        EXPECT_EQ(failedA->Get(), 0);
    }
    auto failedB = FailedCounter("OpB");
    ASSERT_NE(failedB, nullptr);
    EXPECT_EQ(failedB->Get(), 1);
    EXPECT_EQ(DurationHistogram("OpA")->Count(), 1u);
    EXPECT_EQ(DurationHistogram("OpB")->Count(), 1u);
}

TEST_F(RequestMetricsTest, MultipleRequestsAccumulate) {
    for (int i = 0; i < 5; ++i) {
        TRequestMetrics metrics(&OpCollector, "Op", TLog());
        metrics.End(i % 2 == 0 ? EStatus::SUCCESS : EStatus::TIMEOUT);
    }

    auto failed = FailedCounter("Op");
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->Get(), 2);
    EXPECT_EQ(DurationHistogram("Op")->Count(), 5u);
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

    auto failed = FailedCounter("Op");
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->Get(), static_cast<int64_t>(errorStatuses.size()));
}

TEST_F(RequestMetricsTest, DeprecatedAndNonSpecLabelsAreNotEmitted) {
    {
        TRequestMetrics metrics(&OpCollector, "Op", TLog());
        metrics.End(EStatus::UNAVAILABLE);
    }

    EXPECT_EQ(Registry->GetCounter("db.client.operation.requests", FailedLabels("Op")), nullptr);
    EXPECT_EQ(Registry->GetCounter("db.client.operation.errors", FailedLabels("Op")), nullptr);

    auto withExtraLabel = FailedLabels("Op");
    withExtraLabel["ydb.client.api"] = "Unspecified";
    EXPECT_EQ(Registry->GetCounter("db.client.operation.failed", withExtraLabel), nullptr);

    auto withStatusLabel = FailedLabels("Op");
    withStatusLabel["db.response.status_code"] = ToString(EStatus::UNAVAILABLE);
    EXPECT_EQ(Registry->GetCounter("db.client.operation.failed", withStatusLabel), nullptr);

    auto durLabelsWithExtras = DurationLabels("Op");
    durLabelsWithExtras["ydb.client.api"] = "Unspecified";
    EXPECT_EQ(Registry->GetHistogram("db.client.operation.duration", durLabelsWithExtras), nullptr);
}

TEST(RequestMetricsDbNamespaceTest, DifferentNamespacesAreSeparateMetricSeries) {
    auto registry = std::make_shared<TFakeMetricRegistry>();
    TStatCollector::TClientOperationStatCollector collectorA(
        nullptr, "/db/alpha", "", registry, kTestServerAddress, kTestServerPort);
    TStatCollector::TClientOperationStatCollector collectorB(
        nullptr, "/db/beta", "", registry, kTestServerAddress, kTestServerPort);

    {
        TRequestMetrics m(&collectorA, "GetSession", TLog());
        m.End(EStatus::SUCCESS);
    }
    {
        TRequestMetrics m(&collectorB, "GetSession", TLog());
        m.End(EStatus::SUCCESS);
    }

    auto durLabels = [](const char* db) {
        return TLabels{
            {"db.system.name", "ydb"},
            {"db.namespace", db},
            {"db.operation.name", "ydb.GetSession"},
            {"server.address", kTestServerAddress},
            {"server.port", ToString(kTestServerPort)},
        };
    };

    auto durAlpha = registry->GetHistogram("db.client.operation.duration", durLabels("/db/alpha"));
    auto durBeta = registry->GetHistogram("db.client.operation.duration", durLabels("/db/beta"));
    ASSERT_NE(durAlpha, nullptr);
    ASSERT_NE(durBeta, nullptr);
    EXPECT_EQ(durAlpha->Count(), 1u);
    EXPECT_EQ(durBeta->Count(), 1u);
}

TEST(RequestMetricsServerAddressTest, DifferentEndpointsAreSeparateSeries) {
    auto registry = std::make_shared<TFakeMetricRegistry>();
    TStatCollector::TClientOperationStatCollector collectorA(
        nullptr, kTestDbNamespace, "", registry, "host-a", 2135);
    TStatCollector::TClientOperationStatCollector collectorB(
        nullptr, kTestDbNamespace, "", registry, "host-b", 2135);

    {
        TRequestMetrics m(&collectorA, "Op", TLog());
        m.End(EStatus::SUCCESS);
    }
    {
        TRequestMetrics m(&collectorB, "Op", TLog());
        m.End(EStatus::SUCCESS);
    }

    auto labels = [](const char* host) {
        return TLabels{
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", "ydb.Op"},
            {"server.address", host},
            {"server.port", "2135"},
        };
    };
    EXPECT_NE(registry->GetHistogram("db.client.operation.duration", labels("host-a")), nullptr);
    EXPECT_NE(registry->GetHistogram("db.client.operation.duration", labels("host-b")), nullptr);
}

TEST(RequestMetricsEmptyServerTest, OmitsServerLabelsWhenAddressUnset) {
    auto registry = std::make_shared<TFakeMetricRegistry>();
    TStatCollector::TClientOperationStatCollector collector(
        nullptr, kTestDbNamespace, "", registry);

    {
        TRequestMetrics m(&collector, "Op", TLog());
        m.End(EStatus::SUCCESS);
    }

    TLabels labels = {
        {"db.system.name", "ydb"},
        {"db.namespace", kTestDbNamespace},
        {"db.operation.name", "ydb.Op"},
    };
    EXPECT_NE(registry->GetHistogram("db.client.operation.duration", labels), nullptr);
}

// ---------------------------------------------------------------------------
// Discovery endpoint parsing
// ---------------------------------------------------------------------------

TEST(DiscoveryEndpointParserTest, ParsesHostPort) {
    std::string host;
    std::uint16_t port = 0;
    TStatCollector::ParseDiscoveryEndpoint("ydb.example.com:2135", host, port);
    EXPECT_EQ(host, "ydb.example.com");
    EXPECT_EQ(port, 2135);
}

TEST(DiscoveryEndpointParserTest, StripsGrpcScheme) {
    std::string host;
    std::uint16_t port = 0;
    TStatCollector::ParseDiscoveryEndpoint("grpc://h:1234", host, port);
    EXPECT_EQ(host, "h");
    EXPECT_EQ(port, 1234);

    TStatCollector::ParseDiscoveryEndpoint("grpcs://h.example:443", host, port);
    EXPECT_EQ(host, "h.example");
    EXPECT_EQ(port, 443);
}

TEST(DiscoveryEndpointParserTest, ParsesIpv6) {
    std::string host;
    std::uint16_t port = 0;
    TStatCollector::ParseDiscoveryEndpoint("[::1]:2135", host, port);
    EXPECT_EQ(host, "::1");
    EXPECT_EQ(port, 2135);
}

TEST(DiscoveryEndpointParserTest, RejectsBadInput) {
    std::string host = "stale";
    std::uint16_t port = 99;
    TStatCollector::ParseDiscoveryEndpoint("", host, port);
    EXPECT_TRUE(host.empty());
    EXPECT_EQ(port, 0);

    TStatCollector::ParseDiscoveryEndpoint("no-port-here", host, port);
    EXPECT_TRUE(host.empty());
    EXPECT_EQ(port, 0);

    TStatCollector::ParseDiscoveryEndpoint("h:abc", host, port);
    EXPECT_TRUE(host.empty());
    EXPECT_EQ(port, 0);

    TStatCollector::ParseDiscoveryEndpoint("h:99999", host, port);
    EXPECT_TRUE(host.empty());
    EXPECT_EQ(port, 0);
}

// ---------------------------------------------------------------------------
// Session pool / connection metrics (db.client.connection.*)
// ---------------------------------------------------------------------------

namespace {
    NMetrics::TLabels BasePoolLabels(const std::string& database, const std::string& clientType) {
        return {
            {"db.system.name", "ydb"},
            {"db.namespace", database},
            {"db.client.connection.pool.name", clientType.empty() ? std::string("Unspecified") : clientType},
        };
    }

    NMetrics::TLabels CountLabels(const std::string& database,
                                  const std::string& clientType,
                                  const std::string& state) {
        auto labels = BasePoolLabels(database, clientType);
        labels["db.client.connection.state"] = state;
        return labels;
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
        BasePoolLabels(kTestDbNamespace, "Query"));
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
        BasePoolLabels(kTestDbNamespace, "Query"));
    ASSERT_NE(counter, nullptr);
    EXPECT_EQ(counter->Get(), 3);
}

TEST_F(ConnectionPoolMetricsTest, ConnectionCountSplitsByState) {
    Collector.UpdateConnectionCount(/*idle=*/8, /*used=*/3);

    auto idle = Registry->GetGauge(
        "db.client.connection.count",
        CountLabels(kTestDbNamespace, "Query", "idle"));
    auto used = Registry->GetGauge(
        "db.client.connection.count",
        CountLabels(kTestDbNamespace, "Query", "used"));
    ASSERT_NE(idle, nullptr);
    ASSERT_NE(used, nullptr);
    EXPECT_DOUBLE_EQ(idle->Get(), 8.0);
    EXPECT_DOUBLE_EQ(used->Get(), 3.0);
}

TEST_F(ConnectionPoolMetricsTest, ConnectionCountUpdates) {
    Collector.UpdateConnectionCount(5, 1);
    Collector.UpdateConnectionCount(2, 4);

    auto idle = Registry->GetGauge(
        "db.client.connection.count",
        CountLabels(kTestDbNamespace, "Query", "idle"));
    auto used = Registry->GetGauge(
        "db.client.connection.count",
        CountLabels(kTestDbNamespace, "Query", "used"));
    ASSERT_NE(idle, nullptr);
    ASSERT_NE(used, nullptr);
    EXPECT_DOUBLE_EQ(idle->Get(), 2.0);
    EXPECT_DOUBLE_EQ(used->Get(), 4.0);
}

TEST_F(ConnectionPoolMetricsTest, ConnectionCountWithoutStateLabelIsNotEmitted) {
    Collector.UpdateConnectionCount(5, 3);

    EXPECT_EQ(Registry->GetGauge(
        "db.client.connection.count",
        BasePoolLabels(kTestDbNamespace, "Query")), nullptr);
}

TEST_F(ConnectionPoolMetricsTest, PendingRequestsGauge) {
    Collector.UpdatePendingRequests(7);

    auto gauge = Registry->GetGauge(
        "db.client.connection.pending_requests",
        BasePoolLabels(kTestDbNamespace, "Query"));
    ASSERT_NE(gauge, nullptr);
    EXPECT_DOUBLE_EQ(gauge->Get(), 7.0);
}

TEST_F(ConnectionPoolMetricsTest, PoolMetricsHaveNoYdbClientApiLabel) {
    Collector.IncConnectionTimeouts();
    Collector.RecordConnectionCreateTime(0.01);
    Collector.UpdatePendingRequests(1);
    Collector.UpdateConnectionCount(1, 1);

    auto withExtra = BasePoolLabels(kTestDbNamespace, "Query");
    withExtra["ydb.client.api"] = "Query";

    EXPECT_EQ(Registry->GetCounter("db.client.connection.timeouts", withExtra), nullptr);
    EXPECT_EQ(Registry->GetHistogram("db.client.connection.create_time", withExtra), nullptr);
    EXPECT_EQ(Registry->GetGauge("db.client.connection.pending_requests", withExtra), nullptr);
}

TEST(ConnectionPoolMetricsNoRegistryTest, NullRegistryIsSafe) {
    TStatCollector::TSessionPoolStatCollector collector;
    EXPECT_FALSE(collector.HasExternalRegistry());
    EXPECT_NO_THROW({
        collector.RecordConnectionCreateTime(1.0);
        collector.IncConnectionTimeouts();
        collector.UpdateConnectionCount(3, 1);
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
        BasePoolLabels(kTestDbNamespace, "Query"));
    auto tableCounter = registry->GetCounter(
        "db.client.connection.timeouts",
        BasePoolLabels(kTestDbNamespace, "Table"));

    ASSERT_NE(queryCounter, nullptr);
    ASSERT_NE(tableCounter, nullptr);
    EXPECT_EQ(queryCounter->Get(), 1);
    EXPECT_EQ(tableCounter->Get(), 2);
}

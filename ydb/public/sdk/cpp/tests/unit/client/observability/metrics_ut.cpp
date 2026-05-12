#include <ydb/public/sdk/cpp/src/client/impl/observability/metrics.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/span.h>
#include <ydb/public/sdk/cpp/src/client/impl/stats/stats.h>
#include <ydb/public/sdk/cpp/tests/common/fake_metric_registry.h>
#include <ydb/public/sdk/cpp/tests/common/fake_trace_provider.h>
#include <util/string/cast.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <algorithm>

using namespace NYdb;
using namespace NYdb::NObservability;
using namespace NYdb::NMetrics;
using namespace NYdb::NTests;
using namespace NYdb::NSdkStats;

namespace {
    constexpr const char kTestDbNamespace[] = "/Root/testdb";
    constexpr const char kTestServerAddress[] = "ydb.example.com";
    constexpr std::uint16_t kTestServerPort = 2135;
    constexpr const char kTestPoolName[] = "/Root/testdb@grpc://localhost:2135";
} // namespace

// ---------------------------------------------------------------------------
// TRequestMetrics (ydb.client.operation.* — strict OTel-style label set)
// ---------------------------------------------------------------------------

class RequestMetricsTest : public ::testing::Test {
protected:
    void SetUp() override {
        Registry = std::make_shared<TFakeMetricRegistry>();
        OpCollector = TStatCollector::TClientOperationStatCollector(
            nullptr, kTestDbNamespace, "", Registry, kTestServerAddress, kTestServerPort);
    }

    static TLabels FailedLabels(const std::string& op, EStatus status) {
        return {
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", op},
            {"db.response.status_code", ToString(status)},
            {"server.address", kTestServerAddress},
            {"server.port", ToString(kTestServerPort)},
        };
    }

    static TLabels DurationLabels(const std::string& op) {
        return {
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", op},
            {"server.address", kTestServerAddress},
            {"server.port", ToString(kTestServerPort)},
        };
    }

    std::shared_ptr<TFakeCounter> FailedCounter(const std::string& op, EStatus status) {
        return Registry->GetCounter("ydb.client.operation.failed", FailedLabels(op, status));
    }

    std::shared_ptr<TFakeHistogram> DurationHistogram(const std::string& op) {
        return Registry->GetHistogram("ydb.client.operation.duration", DurationLabels(op));
    }

    TStatCollector::TClientOperationStatCollector OpCollector;
    std::shared_ptr<TFakeMetricRegistry> Registry;
};

TEST_F(RequestMetricsTest, SuccessDoesNotIncrementFailedCounter) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::SUCCESS);
    }

    // The failed counter must not exist for any status when we only ran SUCCESS.
    auto failed = FailedCounter("DoSomething", EStatus::UNAVAILABLE);
    EXPECT_EQ(failed, nullptr);
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

    EXPECT_EQ(FailedCounter("DoSomething", EStatus::INTERNAL_ERROR), nullptr);

    auto hist = DurationHistogram("DoSomething");
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

TEST_F(RequestMetricsTest, FailedCounterCarriesStatusCodeLabel) {
    {
        TRequestMetrics m(&OpCollector, "Op", TLog());
        m.End(EStatus::OVERLOADED);
    }
    {
        TRequestMetrics m(&OpCollector, "Op", TLog());
        m.End(EStatus::TIMEOUT);
    }

    auto overloaded = FailedCounter("Op", EStatus::OVERLOADED);
    auto timeout = FailedCounter("Op", EStatus::TIMEOUT);
    ASSERT_NE(overloaded, nullptr);
    ASSERT_NE(timeout, nullptr);
    EXPECT_EQ(overloaded->Get(), 1);
    EXPECT_EQ(timeout->Get(), 1);
}

TEST_F(RequestMetricsTest, MultipleSameStatusesAccumulateOnSameSeries) {
    for (int i = 0; i < 4; ++i) {
        TRequestMetrics metrics(&OpCollector, "Op", TLog());
        metrics.End(EStatus::TIMEOUT);
    }

    auto failed = FailedCounter("Op", EStatus::TIMEOUT);
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->Get(), 4);
    EXPECT_EQ(DurationHistogram("Op")->Count(), 4u);
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

    for (auto st : errorStatuses) {
        auto failed = FailedCounter("Op", st);
        ASSERT_NE(failed, nullptr) << "no failed counter for status " << ToString(st);
        EXPECT_EQ(failed->Get(), 1);
    }
}

TEST_F(RequestMetricsTest, LegacyMetricNamesAreNotEmitted) {
    {
        TRequestMetrics metrics(&OpCollector, "Op", TLog());
        metrics.End(EStatus::UNAVAILABLE);
    }

    EXPECT_EQ(Registry->GetCounter("db.client.operation.failed", FailedLabels("Op", EStatus::UNAVAILABLE)), nullptr);
    EXPECT_EQ(Registry->GetHistogram("db.client.operation.duration", DurationLabels("Op")), nullptr);
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
            {"db.operation.name", "GetSession"},
            {"server.address", kTestServerAddress},
            {"server.port", ToString(kTestServerPort)},
        };
    };

    auto durAlpha = registry->GetHistogram("ydb.client.operation.duration", durLabels("/db/alpha"));
    auto durBeta = registry->GetHistogram("ydb.client.operation.duration", durLabels("/db/beta"));
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
            {"db.operation.name", "Op"},
            {"server.address", host},
            {"server.port", "2135"},
        };
    };
    EXPECT_NE(registry->GetHistogram("ydb.client.operation.duration", labels("host-a")), nullptr);
    EXPECT_NE(registry->GetHistogram("ydb.client.operation.duration", labels("host-b")), nullptr);
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
        {"db.operation.name", "Op"},
    };
    EXPECT_NE(registry->GetHistogram("ydb.client.operation.duration", labels), nullptr);
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
// PoolName resolution (M9)
// ---------------------------------------------------------------------------

TEST(PoolNameResolutionTest, ExplicitWins) {
    EXPECT_EQ(
        TStatCollector::ResolvePoolName("explicit", "/db", "host:2135"),
        "explicit"
    );
}

TEST(PoolNameResolutionTest, FallsBackToDatabaseAtEndpoint) {
    EXPECT_EQ(
        TStatCollector::ResolvePoolName("", "/Root/db", "grpcs://host:2135"),
        "/Root/db@grpcs://host:2135"
    );
}

// ---------------------------------------------------------------------------
// Session pool / connection metrics (ydb.{query,table}.session.*)
// ---------------------------------------------------------------------------

namespace {
    NMetrics::TLabels QueryPoolLabels(const std::string& poolName) {
        return {
            {"ydb.query.session.pool.name", poolName},
        };
    }

    NMetrics::TLabels QueryCountLabels(const std::string& poolName, const std::string& state) {
        auto labels = QueryPoolLabels(poolName);
        labels["ydb.query.session.state"] = state;
        return labels;
    }

    NMetrics::TLabels TablePoolLabels(const std::string& poolName) {
        return {
            {"ydb.table.session.pool.name", poolName},
        };
    }
} // namespace

class QueryPoolMetricsTest : public ::testing::Test {
protected:
    void SetUp() override {
        Registry = std::make_shared<TFakeMetricRegistry>();
        Collector = TStatCollector::TSessionPoolStatCollector(
            /*activeSessions=*/nullptr,
            /*inPoolSessions=*/nullptr,
            /*fakeSessions=*/nullptr,
            /*waiters=*/nullptr,
            Registry,
            /*clientType=*/"Query",
            /*poolName=*/kTestPoolName);
    }

    std::shared_ptr<TFakeMetricRegistry> Registry;
    TStatCollector::TSessionPoolStatCollector Collector;
};

TEST_F(QueryPoolMetricsTest, CreateTimeRecorded) {
    Collector.RecordConnectionCreateTime(0.002);
    Collector.RecordConnectionCreateTime(0.100);

    auto hist = Registry->GetHistogram(
        "ydb.query.session.create_time",
        QueryPoolLabels(kTestPoolName)
    );
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->Count(), 2u);
    EXPECT_DOUBLE_EQ(hist->GetValues()[0], 0.002);
    EXPECT_DOUBLE_EQ(hist->GetValues()[1], 0.100);
}

TEST_F(QueryPoolMetricsTest, TimeoutsIncrement) {
    Collector.IncConnectionTimeouts();
    Collector.IncConnectionTimeouts();
    Collector.IncConnectionTimeouts();

    auto counter = Registry->GetCounter(
        "ydb.query.session.timeouts",
        QueryPoolLabels(kTestPoolName)
    );
    ASSERT_NE(counter, nullptr);
    EXPECT_EQ(counter->Get(), 3);
}

TEST_F(QueryPoolMetricsTest, SessionCountSplitsByState) {
    Collector.UpdateConnectionCount(/*idle=*/8, /*used=*/3);

    auto idle = Registry->GetGauge(
        "ydb.query.session.count",
        QueryCountLabels(kTestPoolName, "idle")
    );
    auto used = Registry->GetGauge(
        "ydb.query.session.count",
        QueryCountLabels(kTestPoolName, "used")
    );
    ASSERT_NE(idle, nullptr);
    ASSERT_NE(used, nullptr);
    EXPECT_DOUBLE_EQ(idle->Get(), 8.0);
    EXPECT_DOUBLE_EQ(used->Get(), 3.0);
}

TEST_F(QueryPoolMetricsTest, SessionCountUpdates) {
    Collector.UpdateConnectionCount(5, 1);
    Collector.UpdateConnectionCount(2, 4);

    auto idle = Registry->GetGauge(
        "ydb.query.session.count",
        QueryCountLabels(kTestPoolName, "idle")
    );
    auto used = Registry->GetGauge(
        "ydb.query.session.count",
        QueryCountLabels(kTestPoolName, "used")
    );
    ASSERT_NE(idle, nullptr);
    ASSERT_NE(used, nullptr);
    EXPECT_DOUBLE_EQ(idle->Get(), 2.0);
    EXPECT_DOUBLE_EQ(used->Get(), 4.0);
}

TEST_F(QueryPoolMetricsTest, PendingRequestsCounter) {
    Collector.IncPendingRequests();
    Collector.IncPendingRequests();
    Collector.IncPendingRequests();

    auto counter = Registry->GetCounter(
        "ydb.query.session.pending_requests",
        QueryPoolLabels(kTestPoolName)
    );
    ASSERT_NE(counter, nullptr);
    EXPECT_EQ(counter->Get(), 3);

    EXPECT_EQ(Registry->GetGauge("ydb.query.session.pending_requests", QueryPoolLabels(kTestPoolName)), nullptr);
}

TEST_F(QueryPoolMetricsTest, MinMaxLimitsEmitted) {
    Collector.RecordPoolLimits(/*minPoolSize=*/5, /*maxPoolSize=*/100);

    auto minGauge = Registry->GetGauge("ydb.query.session.min", QueryPoolLabels(kTestPoolName));
    auto maxGauge = Registry->GetGauge("ydb.query.session.max", QueryPoolLabels(kTestPoolName));
    ASSERT_NE(minGauge, nullptr);
    ASSERT_NE(maxGauge, nullptr);
    EXPECT_DOUBLE_EQ(minGauge->Get(), 5.0);
    EXPECT_DOUBLE_EQ(maxGauge->Get(), 100.0);
}

TEST_F(QueryPoolMetricsTest, LegacyDbClientConnectionMetricsAreNotEmitted) {
    Collector.IncConnectionTimeouts();
    Collector.RecordConnectionCreateTime(0.01);
    Collector.IncPendingRequests();
    Collector.UpdateConnectionCount(1, 1);
    Collector.RecordPoolLimits(1, 2);

    NMetrics::TLabels legacy = {
        {"db.system.name", "ydb"},
        {"db.namespace", kTestDbNamespace},
        {"db.client.connection.pool.name", "Query"},
    };
    EXPECT_EQ(Registry->GetCounter("db.client.connection.timeouts", legacy), nullptr);
    EXPECT_EQ(Registry->GetHistogram("db.client.connection.create_time", legacy), nullptr);
    EXPECT_EQ(Registry->GetCounter("db.client.connection.pending_requests", legacy), nullptr);
    EXPECT_EQ(Registry->GetGauge("db.client.connection.count", legacy), nullptr);
}

TEST(TablePoolMetricsTest, UsesYdbTableSessionNamespace) {
    auto registry = std::make_shared<TFakeMetricRegistry>();
    TStatCollector::TSessionPoolStatCollector collector(
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        registry,
        /*clientType=*/"Table",
        /*poolName=*/kTestPoolName
    );

    collector.IncConnectionTimeouts();
    collector.RecordPoolLimits(7, 70);

    auto timeouts = registry->GetCounter("ydb.table.session.timeouts", TablePoolLabels(kTestPoolName));
    ASSERT_NE(timeouts, nullptr);
    EXPECT_EQ(timeouts->Get(), 1);

    auto minGauge = registry->GetGauge("ydb.table.session.min", TablePoolLabels(kTestPoolName));
    auto maxGauge = registry->GetGauge("ydb.table.session.max", TablePoolLabels(kTestPoolName));
    ASSERT_NE(minGauge, nullptr);
    ASSERT_NE(maxGauge, nullptr);
    EXPECT_DOUBLE_EQ(minGauge->Get(), 7.0);
    EXPECT_DOUBLE_EQ(maxGauge->Get(), 70.0);

    // Query-namespace metrics must not be emitted for a Table client.
    EXPECT_EQ(registry->GetCounter("ydb.query.session.timeouts", TablePoolLabels(kTestPoolName)), nullptr);
}

TEST(QueryPoolMetricsNoRegistryTest, NullRegistryIsSafe) {
    TStatCollector::TSessionPoolStatCollector collector;
    EXPECT_FALSE(collector.HasExternalRegistry());
    EXPECT_NO_THROW({
        collector.RecordConnectionCreateTime(1.0);
        collector.IncConnectionTimeouts();
        collector.UpdateConnectionCount(3, 1);
        collector.IncPendingRequests();
        collector.RecordPoolLimits(1, 5);
    });
}

TEST(QueryPoolMetricsPoolNameTest, DifferentPoolNamesAreSeparateSeries) {
    auto registry = std::make_shared<TFakeMetricRegistry>();
    TStatCollector::TSessionPoolStatCollector poolA(nullptr, nullptr, nullptr, nullptr, registry, "Query", "alpha");
    TStatCollector::TSessionPoolStatCollector poolB(nullptr, nullptr, nullptr, nullptr, registry, "Query", "beta");

    poolA.IncConnectionTimeouts();
    poolB.IncConnectionTimeouts();
    poolB.IncConnectionTimeouts();

    auto a = registry->GetCounter("ydb.query.session.timeouts", QueryPoolLabels("alpha"));
    auto b = registry->GetCounter("ydb.query.session.timeouts", QueryPoolLabels("beta"));
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(a->Get(), 1);
    EXPECT_EQ(b->Get(), 2);
}

// ---------------------------------------------------------------------------
// Cross-validation: trace spans <-> operation metrics.
// ---------------------------------------------------------------------------

namespace {

struct TOpScenario {
    std::string Op;
    std::vector<EStatus> Statuses;
};

std::size_t CountSpans(const std::vector<TFakeTracer::TSpanRecord>& spans, const std::string& name) {
    return std::count_if(spans.begin(), spans.end(),
        [&](const TFakeTracer::TSpanRecord& r) { return r.Name == name; });
}

std::size_t CountSpansWithException(const std::vector<TFakeTracer::TSpanRecord>& spans,
                                    const std::string& name) {
    return std::count_if(spans.begin(), spans.end(),
        [&](const TFakeTracer::TSpanRecord& r) {
            if (r.Name != name) {
                return false;
            }
            const auto events = r.Span->GetEvents();
            return std::any_of(events.begin(), events.end(),
                [](const TFakeEvent& e) { return e.Name == "exception"; });
        });
}

} // namespace

class MetricsTracesCorrelationTest : public ::testing::Test {
protected:
    void SetUp() override {
        Registry = std::make_shared<TFakeMetricRegistry>();
        Tracer = std::make_shared<TFakeTracer>();
        OpCollector = TStatCollector::TClientOperationStatCollector(
            /*registry=*/nullptr,
            kTestDbNamespace,
            /*ydbClientType=*/"",
            Registry,
            kTestServerAddress,
            kTestServerPort);
        Endpoint = std::string(kTestServerAddress) + ":" + ToString(kTestServerPort);
    }

    void EmitOperation(const std::string& op, EStatus status) {
        auto span = NObservability::TRequestSpan::Create(
            /*ydbClientType=*/"",
            Tracer,
            op,
            Endpoint,
            kTestDbNamespace,
            TLog{});
        TRequestMetrics metrics(&OpCollector, op, TLog{});
        metrics.End(status);
        span->End(status);
    }

    static TLabels DurationLabels(const std::string& op) {
        return {
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", op},
            {"server.address", kTestServerAddress},
            {"server.port", ToString(kTestServerPort)},
        };
    }

    std::shared_ptr<TFakeMetricRegistry> Registry;
    std::shared_ptr<TFakeTracer> Tracer;
    TStatCollector::TClientOperationStatCollector OpCollector;
    std::string Endpoint;
};

TEST_F(MetricsTracesCorrelationTest, DurationCountMatchesSpanCount) {
    const std::vector<TOpScenario> scenarios = {
        {"ExecuteQuery",     std::vector<EStatus>(7, EStatus::SUCCESS)},
        {"BeginTransaction", {EStatus::SUCCESS, EStatus::SUCCESS, EStatus::SUCCESS}},
        {"Commit",           {EStatus::SUCCESS, EStatus::OVERLOADED}},
        {"CreateSession",    {EStatus::SUCCESS, EStatus::SUCCESS, EStatus::TIMEOUT, EStatus::SUCCESS}},
    };

    for (const auto& s : scenarios) {
        for (auto st : s.Statuses) {
            EmitOperation(s.Op, st);
        }
    }

    const auto spans = Tracer->GetSpans();
    for (const auto& s : scenarios) {
        auto hist = Registry->GetHistogram("ydb.client.operation.duration", DurationLabels(s.Op));
        ASSERT_NE(hist, nullptr) << "no histogram for " << s.Op;

        const std::size_t expected = s.Statuses.size();
        EXPECT_EQ(hist->Count(), expected) << s.Op;
        EXPECT_EQ(CountSpans(spans, s.Op), expected) << s.Op;
        EXPECT_EQ(hist->Count(), CountSpans(spans, s.Op))
            << "histogram _count must match span count for " << s.Op;
    }
}

TEST_F(MetricsTracesCorrelationTest, FailedCounterMatchesSpanExceptionEventCount) {
    EmitOperation("OpX", EStatus::SUCCESS);
    EmitOperation("OpX", EStatus::OVERLOADED);
    EmitOperation("OpX", EStatus::OVERLOADED);
    EmitOperation("OpX", EStatus::SUCCESS);

    EmitOperation("OpY", EStatus::ABORTED);
    EmitOperation("OpY", EStatus::SUCCESS);

    EmitOperation("OpZ", EStatus::SUCCESS);
    EmitOperation("OpZ", EStatus::SUCCESS);

    const auto spans = Tracer->GetSpans();

    auto failedX = Registry->GetCounter(
        "ydb.client.operation.failed",
        TLabels{
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", "OpX"},
            {"db.response.status_code", ToString(EStatus::OVERLOADED)},
            {"server.address", kTestServerAddress},
            {"server.port", ToString(kTestServerPort)},
        }
    );
    ASSERT_NE(failedX, nullptr);
    EXPECT_EQ(failedX->Get(), 2);
    EXPECT_EQ(CountSpansWithException(spans, "OpX"), 2u);

    auto failedY = Registry->GetCounter(
        "ydb.client.operation.failed",
        TLabels{
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", "OpY"},
            {"db.response.status_code", ToString(EStatus::ABORTED)},
            {"server.address", kTestServerAddress},
            {"server.port", ToString(kTestServerPort)},
        }
    );
    ASSERT_NE(failedY, nullptr);
    EXPECT_EQ(failedY->Get(), 1);
    EXPECT_EQ(CountSpansWithException(spans, "OpY"), 1u);

    EXPECT_EQ(CountSpansWithException(spans, "OpZ"), 0u);
}

TEST_F(MetricsTracesCorrelationTest, SuccessfulOpsHaveNoExceptionEventNorFailedIncrement) {
    constexpr int kIterations = 12;
    for (int i = 0; i < kIterations; ++i) {
        EmitOperation("Hot", EStatus::SUCCESS);
    }

    const auto spans = Tracer->GetSpans();
    EXPECT_EQ(CountSpans(spans, "Hot"), static_cast<std::size_t>(kIterations));
    EXPECT_EQ(CountSpansWithException(spans, "Hot"), 0u);

    auto hist = Registry->GetHistogram("ydb.client.operation.duration", DurationLabels("Hot"));
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->Count(), static_cast<std::size_t>(kIterations));
}

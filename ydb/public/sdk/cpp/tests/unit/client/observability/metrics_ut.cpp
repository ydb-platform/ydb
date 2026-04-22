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

class RequestMetricsTest : public ::testing::Test {
protected:
    void SetUp() override {
        Registry = std::make_shared<TFakeMetricRegistry>();
        OpCollector = TStatCollector::TClientOperationStatCollector(
            nullptr, kTestDbNamespace, "", Registry);
    }

    std::shared_ptr<TFakeCounter> RequestCounter(const std::string& op) {
        return Registry->GetCounter("db.client.operation.requests", {
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", YdbOp(op)},
            {"ydb.client.api", "Unspecified"},
        });
    }

    std::shared_ptr<TFakeCounter> ErrorCounter(const std::string& op) {
        return Registry->GetCounter("db.client.operation.errors", {
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", YdbOp(op)},
            {"ydb.client.api", "Unspecified"},
        });
    }

    std::shared_ptr<TFakeHistogram> DurationHistogram(const std::string& op, EStatus status) {
        TLabels labels = {
            {"db.system.name", "ydb"},
            {"db.namespace", kTestDbNamespace},
            {"db.operation.name", YdbOp(op)},
            {"ydb.client.api", "Unspecified"},
            {"db.response.status_code", ToString(status)},
        };
        if (status != EStatus::SUCCESS) {
            labels["error.type"] = ToString(status);
        }
        return Registry->GetHistogram("db.client.operation.duration", labels);
    }

    TStatCollector::TClientOperationStatCollector OpCollector;
    std::shared_ptr<TFakeMetricRegistry> Registry;
};

TEST_F(RequestMetricsTest, RequestCounterIncrementedOnConstruction) {
    TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());

    auto counter = RequestCounter("DoSomething");
    ASSERT_NE(counter, nullptr);
    EXPECT_EQ(counter->Get(), 1);
}

TEST_F(RequestMetricsTest, SuccessDoesNotIncrementErrorCounter) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::SUCCESS);
    }

    auto errors = ErrorCounter("DoSomething");
    ASSERT_NE(errors, nullptr);
    EXPECT_EQ(errors->Get(), 0);
}

TEST_F(RequestMetricsTest, FailureIncrementsErrorCounter) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
        metrics.End(EStatus::UNAVAILABLE);
    }

    auto errors = ErrorCounter("DoSomething");
    ASSERT_NE(errors, nullptr);
    EXPECT_EQ(errors->Get(), 1);
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

    auto errors = ErrorCounter("DoSomething");
    ASSERT_NE(errors, nullptr);
    EXPECT_EQ(errors->Get(), 0);

    auto hist = DurationHistogram("DoSomething", EStatus::SUCCESS);
    ASSERT_NE(hist, nullptr);
    EXPECT_EQ(hist->Count(), 1u);
}

TEST_F(RequestMetricsTest, DestructorCallsEndWithClientInternalError) {
    {
        TRequestMetrics metrics(&OpCollector, "DoSomething", TLog());
    }

    auto requests = RequestCounter("DoSomething");
    ASSERT_NE(requests, nullptr);
    EXPECT_EQ(requests->Get(), 1);

    auto errors = ErrorCounter("DoSomething");
    ASSERT_NE(errors, nullptr);
    EXPECT_EQ(errors->Get(), 1);

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

    EXPECT_EQ(RequestCounter("OpA")->Get(), 1);
    EXPECT_EQ(RequestCounter("OpB")->Get(), 1);
    EXPECT_EQ(ErrorCounter("OpA")->Get(), 0);
    EXPECT_EQ(ErrorCounter("OpB")->Get(), 1);
    EXPECT_EQ(DurationHistogram("OpA", EStatus::SUCCESS)->Count(), 1u);
    EXPECT_EQ(DurationHistogram("OpB", EStatus::OVERLOADED)->Count(), 1u);
}

TEST_F(RequestMetricsTest, MultipleRequestsAccumulate) {
    for (int i = 0; i < 5; ++i) {
        TRequestMetrics metrics(&OpCollector, "Op", TLog());
        metrics.End(i % 2 == 0 ? EStatus::SUCCESS : EStatus::TIMEOUT);
    }

    EXPECT_EQ(RequestCounter("Op")->Get(), 5);
    EXPECT_EQ(ErrorCounter("Op")->Get(), 2);
    EXPECT_EQ(DurationHistogram("Op", EStatus::SUCCESS)->Count(), 3u);
    EXPECT_EQ(DurationHistogram("Op", EStatus::TIMEOUT)->Count(), 2u);
}

TEST_F(RequestMetricsTest, AllErrorStatusesIncrementErrorCounter) {
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

    auto errors = ErrorCounter("Op");
    ASSERT_NE(errors, nullptr);
    EXPECT_EQ(errors->Get(), static_cast<int64_t>(errorStatuses.size()));
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

    auto labelsAlpha = [](const char* op) {
        return NMetrics::TLabels{
            {"db.system.name", "ydb"},
            {"db.namespace", "/db/alpha"},
            {"db.operation.name", YdbOp(op)},
            {"ydb.client.api", "Unspecified"},
        };
    };
    auto labelsBeta = [](const char* op) {
        return NMetrics::TLabels{
            {"db.system.name", "ydb"},
            {"db.namespace", "/db/beta"},
            {"db.operation.name", YdbOp(op)},
            {"ydb.client.api", "Unspecified"},
        };
    };

    auto reqAlpha = registry->GetCounter("db.client.operation.requests", labelsAlpha("ydb.GetSession"));
    auto reqBeta = registry->GetCounter("db.client.operation.requests", labelsBeta("ydb.GetSession"));
    ASSERT_NE(reqAlpha, nullptr);
    ASSERT_NE(reqBeta, nullptr);
    EXPECT_EQ(reqAlpha->Get(), 1);
    EXPECT_EQ(reqBeta->Get(), 1);

    auto durAlpha = registry->GetHistogram(
        "db.client.operation.duration",
        [&] {
            auto l = labelsAlpha("ydb.GetSession");
            l["db.response.status_code"] = ToString(EStatus::SUCCESS);
            return l;
        }());
    auto durBeta = registry->GetHistogram(
        "db.client.operation.duration",
        [&] {
            auto l = labelsBeta("ydb.GetSession");
            l["db.response.status_code"] = ToString(EStatus::SUCCESS);
            return l;
        }());
    ASSERT_NE(durAlpha, nullptr);
    ASSERT_NE(durBeta, nullptr);
    EXPECT_EQ(durAlpha->Count(), 1u);
    EXPECT_EQ(durBeta->Count(), 1u);
}

TEST(RequestMetricsClientAliasesTest, QueryOperationsUseOtelStandardMetrics) {
    auto registry = std::make_shared<TFakeMetricRegistry>();
    TStatCollector::TClientOperationStatCollector collector(nullptr, "", "Query", registry);

    NObservability::TRequestMetrics metrics(&collector, "ExecuteQuery", TLog());
    metrics.End(EStatus::SUCCESS);

    EXPECT_NE(
        registry->GetCounter(
            "db.client.operation.requests",
            {
                {"db.system.name", "ydb"},
                {"db.namespace", ""},
                {"db.operation.name", "ExecuteQuery"},
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
                {"db.operation.name", "ExecuteQuery"},
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
                {"db.operation.name", "ExecuteQuery"},
                {"ydb.client.api", "Query"},
                {"db.response.status_code", ToString(EStatus::SUCCESS)},
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
        registry->GetCounter(
            "db.client.operation.requests",
            {
                {"db.system.name", "ydb"},
                {"db.namespace", ""},
                {"db.operation.name", "ydb.ExecuteDataQuery"},
                {"ydb.client.api", "Table"},
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
                {"db.operation.name", "ydb.ExecuteDataQuery"},
                {"ydb.client.api", "Table"},
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
                {"db.operation.name", "ydb.ExecuteDataQuery"},
                {"ydb.client.api", "Table"},
                {"db.response.status_code", ToString(EStatus::SUCCESS)},
            }
        ),
        nullptr
    );
}

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/open_telemetry/metrics.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>

#include <opentelemetry/sdk/metrics/data/metric_data.h>
#include <opentelemetry/sdk/metrics/data/point_data.h>
#include <opentelemetry/sdk/metrics/export/metric_producer.h>
#include <opentelemetry/sdk/metrics/instruments.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/metric_reader.h>
#include <opentelemetry/sdk/metrics/view/view_registry.h>
#include <opentelemetry/sdk/resource/resource.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <chrono>

namespace nostd       = opentelemetry::nostd;
namespace sdkmetrics  = opentelemetry::sdk::metrics;
namespace sdkresource = opentelemetry::sdk::resource;
namespace otmetrics   = opentelemetry::metrics;

using namespace NYdb;
using namespace NYdb::NMetrics;

namespace {
class TInMemoryMetricReader : public sdkmetrics::MetricReader {
public:
    sdkmetrics::AggregationTemporality GetAggregationTemporality(
        sdkmetrics::InstrumentType /*instrument_type*/) const noexcept override
    {
        return sdkmetrics::AggregationTemporality::kCumulative;
    }

private:
    bool OnForceFlush(std::chrono::microseconds /*timeout*/) noexcept override { return true; }
    bool OnShutDown(std::chrono::microseconds /*timeout*/) noexcept override { return true; }
};

struct TFixture {
    std::shared_ptr<sdkmetrics::MeterProvider> RawProvider;
    TInMemoryMetricReader* Reader = nullptr;
    std::shared_ptr<IMetricRegistry> Registry;
};

TFixture MakeFixture() {
    TFixture f;

    auto reader = std::make_unique<TInMemoryMetricReader>();
    f.Reader = reader.get();

    f.RawProvider = std::make_shared<sdkmetrics::MeterProvider>(
        std::unique_ptr<sdkmetrics::ViewRegistry>(new sdkmetrics::ViewRegistry()),
        sdkresource::Resource::Create({}));
    f.RawProvider->AddMetricReader(std::move(reader));

    nostd::shared_ptr<otmetrics::MeterProvider> apiProvider(
        std::shared_ptr<otmetrics::MeterProvider>(f.RawProvider));
    f.Registry = CreateOtelMetricRegistry(apiProvider);
    return f;
}

std::optional<double> ReadSumPointDouble(TInMemoryMetricReader* reader, const std::string& name) {
    std::optional<double> result;
    reader->Collect([&](sdkmetrics::ResourceMetrics& rm) -> bool {
        for (const auto& sm : rm.scope_metric_data_) {
            for (const auto& md : sm.metric_data_) {
                if (md.instrument_descriptor.name_ != name) {
                    continue;
                }
                EXPECT_EQ(md.point_data_attr_.size(), 1u)
                    << "expected single time series for " << name;
                if (md.point_data_attr_.empty()) {
                    continue;
                }
                const auto& point = md.point_data_attr_.front().point_data;
                if (auto* sum = nostd::get_if<sdkmetrics::SumPointData>(&point)) {
                    if (auto* dv = nostd::get_if<double>(&sum->value_)) {
                        result = *dv;
                    }
                }
            }
        }
        return true;
    });
    return result;
}

} // namespace

TEST(OtelRegistryCaching, GaugeReturnsSameInstanceForSameKey) {
    auto f = MakeFixture();

    auto g1 = f.Registry->Gauge("test.gauge", {{"k", "v"}}, "desc", "1");
    auto g2 = f.Registry->Gauge("test.gauge", {{"k", "v"}}, "desc", "1");
    ASSERT_NE(g1, nullptr);
    EXPECT_EQ(g1.get(), g2.get())
        << "TOtelMetricRegistry must reuse IGauge wrappers, otherwise Set() accumulates";
}

TEST(OtelRegistryCaching, DifferentLabelsYieldDistinctInstances) {
    auto f = MakeFixture();

    auto g1 = f.Registry->Gauge("test.gauge", {{"state", "idle"}}, "d", "1");
    auto g2 = f.Registry->Gauge("test.gauge", {{"state", "used"}}, "d", "1");
    ASSERT_NE(g1, nullptr);
    ASSERT_NE(g2, nullptr);
    EXPECT_NE(g1.get(), g2.get());
}

TEST(OtelRegistryCaching, CounterAndHistogramAreAlsoCached) {
    auto f = MakeFixture();

    auto c1 = f.Registry->Counter("test.counter", {{"k", "v"}}, "d", "1");
    auto c2 = f.Registry->Counter("test.counter", {{"k", "v"}}, "d", "1");
    EXPECT_EQ(c1.get(), c2.get());

    auto h1 = f.Registry->Histogram("test.hist", {0.001, 0.01, 0.1}, {{"k", "v"}}, "d", "s");
    auto h2 = f.Registry->Histogram("test.hist", {0.001, 0.01, 0.1}, {{"k", "v"}}, "d", "s");
    EXPECT_EQ(h1.get(), h2.get());
}


TEST(OtelRegistryGaugeBehaviour, RepeatedSetReplacesUnderlyingValue) {
    auto f = MakeFixture();

    auto gauge = f.Registry->Gauge(
        "db.client.connection.count",
        {{"db.client.connection.state", "idle"}, {"db.client.connection.pool.name", "Query"}},
        "Number of connections currently in the pool",
        "{connection}");

    gauge->Set(5);
    gauge->Set(3);
    gauge->Set(8);

    auto value = ReadSumPointDouble(f.Reader, "db.client.connection.count");
    ASSERT_TRUE(value.has_value()) << "metric was not produced by reader";
    EXPECT_DOUBLE_EQ(*value, 8.0)
        << "Gauge with cached wrapper must converge to the last Set() value, not the sum";
}

TEST(OtelRegistryGaugeBehaviour, ZigZagSetsNeverDriveValueAboveLastSet) {
    auto f = MakeFixture();

    auto gauge = f.Registry->Gauge(
        "test.zigzag.gauge", {{"k", "v"}}, "d", "1");

    const std::vector<double> writes = {1, 7, 2, 9, 4, 6, 3, 11, 0, 5};
    for (double v : writes) {
        gauge->Set(v);
        auto current = ReadSumPointDouble(f.Reader, "test.zigzag.gauge");
        ASSERT_TRUE(current.has_value());
        EXPECT_DOUBLE_EQ(*current, v)
            << "after Set(" << v << ") the exposed value must be " << v
            << ", not a running sum";
    }
}

TEST(OtelRegistryGaugeBehaviour, IndependentGaugesDoNotCrossTalk) {
    auto f = MakeFixture();

    auto idle = f.Registry->Gauge(
        "db.client.connection.count",
        {{"db.client.connection.state", "idle"}, {"db.client.connection.pool.name", "Query"}},
        "d", "{connection}");
    auto used = f.Registry->Gauge(
        "db.client.connection.count",
        {{"db.client.connection.state", "used"}, {"db.client.connection.pool.name", "Query"}},
        "d", "{connection}");

    idle->Set(10);
    used->Set(2);
    idle->Set(7);
    used->Set(5);
    idle->Set(4);

    std::map<std::string, double> byState;
    f.Reader->Collect([&](sdkmetrics::ResourceMetrics& rm) -> bool {
        for (const auto& sm : rm.scope_metric_data_) {
            for (const auto& md : sm.metric_data_) {
                if (md.instrument_descriptor.name_ != "db.client.connection.count") {
                    continue;
                }
                for (const auto& pa : md.point_data_attr_) {
                    auto stateIt = pa.attributes.find("db.client.connection.state");
                    if (stateIt == pa.attributes.end()) {
                        continue;
                    }
                    const auto* state = nostd::get_if<std::string>(&stateIt->second);
                    const auto* sum = nostd::get_if<sdkmetrics::SumPointData>(&pa.point_data);
                    if (!state || !sum) {
                        continue;
                    }
                    const auto* dv = nostd::get_if<double>(&sum->value_);
                    if (!dv) {
                        continue;
                    }
                    byState[*state] = *dv;
                }
            }
        }
        return true;
    });

    EXPECT_EQ(byState.size(), 2u);
    EXPECT_DOUBLE_EQ(byState["idle"], 4.0);
    EXPECT_DOUBLE_EQ(byState["used"], 5.0);
}

TEST(OtelRegistryGaugeBehaviour, AddAndSetInteroperateWithoutAccumulation) {
    auto f = MakeFixture();

    auto gauge = f.Registry->Gauge("test.add_set", {{"k", "v"}}, "d", "1");

    gauge->Set(10);
    gauge->Add(2);   // 10 -> 12
    gauge->Add(-5);  // 12 -> 7
    gauge->Set(4);   // -> 4

    auto value = ReadSumPointDouble(f.Reader, "test.add_set");
    ASSERT_TRUE(value.has_value());
    EXPECT_DOUBLE_EQ(*value, 4.0);
}

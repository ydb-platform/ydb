#include "metric_registry.h"

#include <library/cpp/monlib/encode/protobuf/protobuf.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/resource/resource.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

using namespace NMonitoring;

template<>
void Out<NMonitoring::NProto::TSingleSample::ValueCase>(IOutputStream& os, NMonitoring::NProto::TSingleSample::ValueCase val) {
    switch (val) {
    case NMonitoring::NProto::TSingleSample::ValueCase::kInt64:
        os << "Int64";
        break;
    case NMonitoring::NProto::TSingleSample::ValueCase::kUint64:
        os << "Uint64";
        break;
    case NMonitoring::NProto::TSingleSample::ValueCase::kHistogram:
        os << "Histogram";
        break;
    case NMonitoring::NProto::TSingleSample::ValueCase::kFloat64:
        os << "Float64";
        break;
    case NMonitoring::NProto::TSingleSample::ValueCase::kSummaryDouble:
        os << "DSummary";
        break;
    case NMonitoring::NProto::TSingleSample::ValueCase::kLogHistogram:
        os << "LogHistogram";
        break;
    case NMonitoring::NProto::TSingleSample::ValueCase::VALUE_NOT_SET:
        os << "NOT SET";
        break;
    }
}

namespace {
    template<typename F>
    auto EnsureIdempotent(F&& f) {
        auto firstResult = f();
        auto secondResult = f();
        UNIT_ASSERT_VALUES_EQUAL(firstResult, secondResult);
        return secondResult;
    }
}

Y_UNIT_TEST_SUITE(TMetricRegistryTest) {
    Y_UNIT_TEST(Gauge) {
        TMetricRegistry registry(TLabels{{"common", "label"}});
        TGauge* g = EnsureIdempotent([&] { return registry.Gauge({{"my", "gauge"}}); });

        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 0.0, 1E-6);
        g->Set(12.34);
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 12.34, 1E-6);

        double val;

        val = g->Add(1.2);
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 13.54, 1E-6);
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), val, 1E-6);

        val = g->Add(-3.47);
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 10.07, 1E-6);
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), val, 1E-6);
    }

    Y_UNIT_TEST(LazyGauge) {
        TMetricRegistry registry(TLabels{{"common", "label"}});
        double val = 0.0;
        TLazyGauge* g = EnsureIdempotent([&] { return registry.LazyGauge({{"my", "lazyGauge"}}, [&val](){return val;}); });

        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 0.0, 1E-6);
        val = 12.34;
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 12.34, 1E-6);

        val += 1.2;
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 13.54, 1E-6);
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), val, 1E-6);

        val += -3.47;
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), 10.07, 1E-6);
        UNIT_ASSERT_DOUBLES_EQUAL(g->Get(), val, 1E-6);
    }

    Y_UNIT_TEST(IntGauge) {
        TMetricRegistry registry(TLabels{{"common", "label"}});
        TIntGauge* g = EnsureIdempotent([&] { return registry.IntGauge({{"my", "gauge"}}); });

        UNIT_ASSERT_VALUES_EQUAL(g->Get(), 0);

        i64 val;

        val = g->Inc();
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), 1);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), val);

        val = g->Dec();
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), 0);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), val);

        val = g->Add(1);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), 1);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), val);

        val = g->Add(2);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), 3);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), val);

        val = g->Add(-5);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), -2);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), val);
    }

    Y_UNIT_TEST(LazyIntGauge) {
        TMetricRegistry registry(TLabels{{"common", "label"}});
        i64 val = 0;
        TLazyIntGauge* g = EnsureIdempotent([&] { return registry.LazyIntGauge({{"my", "gauge"}}, [&val](){return val;}); });

        UNIT_ASSERT_VALUES_EQUAL(g->Get(), 0);
        val += 1;
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), 1);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), val);

        val -= 1;
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), 0);
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), val);

        val = 42;
        UNIT_ASSERT_VALUES_EQUAL(g->Get(), val);
    }

    Y_UNIT_TEST(Counter) {
        TMetricRegistry registry(TLabels{{"common", "label"}});
        TCounter* c = EnsureIdempotent([&] { return registry.Counter({{"my", "counter"}}); });

        UNIT_ASSERT_VALUES_EQUAL(c->Get(), 0);
        UNIT_ASSERT_VALUES_EQUAL(c->Inc(), 1);
        UNIT_ASSERT_VALUES_EQUAL(c->Get(), 1);
        UNIT_ASSERT_VALUES_EQUAL(c->Add(10), 11);
        UNIT_ASSERT_VALUES_EQUAL(c->Get(), 11);
    }

    Y_UNIT_TEST(LazyCounter) {
        TMetricRegistry registry(TLabels{{"common", "label"}});
        ui64 val = 0;

        TLazyCounter* c = EnsureIdempotent([&] { return registry.LazyCounter({{"my", "counter"}}, [&val](){return val;}); });

        UNIT_ASSERT_VALUES_EQUAL(c->Get(), 0);
        val = 42;
        UNIT_ASSERT_VALUES_EQUAL(c->Get(), 42);
    }

    Y_UNIT_TEST(LazyRate) {
        TMetricRegistry registry(TLabels{{"common", "label"}});
        ui64 val = 0;

        TLazyRate* r = EnsureIdempotent([&] { return registry.LazyRate({{"my", "rate"}}, [&val](){return val;}); });

        UNIT_ASSERT_VALUES_EQUAL(r->Get(), 0);
        val = 42;
        UNIT_ASSERT_VALUES_EQUAL(r->Get(), 42);
    }

    Y_UNIT_TEST(DoubleCounter) {
        TMetricRegistry registry(TLabels{{"common", "label"}});

        TCounter* c = registry.Counter({{"my", "counter"}});
        UNIT_ASSERT_VALUES_EQUAL(c->Get(), 0);
        c->Add(10);

        c = registry.Counter({{"my", "counter"}});
        UNIT_ASSERT_VALUES_EQUAL(c->Get(), 10);
    }

    Y_UNIT_TEST(Sample) {
        TMetricRegistry registry(TLabels{{"common", "label"}});

        TGauge* g = registry.Gauge({{"my", "gauge"}});
        g->Set(12.34);

        TCounter* c = registry.Counter({{"my", "counter"}});
        c->Add(10);

        NProto::TSingleSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);
        auto now = TInstant::Now();
        registry.Accept(now, encoder.Get());

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(samples.CommonLabelsSize(), 1);
        {
            const NProto::TLabel& label = samples.GetCommonLabels(0);
            UNIT_ASSERT_STRINGS_EQUAL(label.GetName(), "common");
            UNIT_ASSERT_STRINGS_EQUAL(label.GetValue(), "label");
        }


        for (const NProto::TSingleSample& sample : samples.GetSamples()) {
            UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(sample.GetTime(), now.MilliSeconds());

            if (sample.GetMetricType() == NProto::GAUGE) {
                UNIT_ASSERT_VALUES_EQUAL(sample.GetValueCase(), NProto::TSingleSample::kFloat64);
                UNIT_ASSERT_DOUBLES_EQUAL(sample.GetFloat64(), 12.34, 1E-6);

                const NProto::TLabel& label = sample.GetLabels(0);
                UNIT_ASSERT_STRINGS_EQUAL(label.GetName(), "my");
                UNIT_ASSERT_STRINGS_EQUAL(label.GetValue(), "gauge");
            } else if (sample.GetMetricType() == NProto::COUNTER) {
                UNIT_ASSERT_VALUES_EQUAL(sample.GetValueCase(), NProto::TSingleSample::kUint64);
                UNIT_ASSERT_VALUES_EQUAL(sample.GetUint64(), 10);

                const NProto::TLabel& label = sample.GetLabels(0);
                UNIT_ASSERT_STRINGS_EQUAL(label.GetName(), "my");
                UNIT_ASSERT_STRINGS_EQUAL(label.GetValue(), "counter");
            } else {
                UNIT_FAIL("unexpected sample type");
            }
        }
    }

    Y_UNIT_TEST(Histograms) {
        TMetricRegistry registry(TLabels{{"common", "label"}});

        THistogram* h1 = EnsureIdempotent([&] {
            return registry.HistogramCounter(
                {{"sensor", "readTimeMillis"}},
                ExponentialHistogram(5, 2));
        });

        THistogram* h2 = EnsureIdempotent([&] {
            return registry.HistogramRate(
                {{"sensor", "writeTimeMillis"}},
                ExplicitHistogram({1, 5, 15, 20, 25}));
        });

        for (i64 i = 0; i < 100; i++) {
            h1->Record(i);
            h2->Record(i);
        }

        TStringStream ss;
        {
            auto encoder = EncoderJson(&ss, 2);
            registry.Accept(TInstant::Zero(), encoder.Get());
        }
        ss << '\n';

        UNIT_ASSERT_NO_DIFF(ss.Str(), NResource::Find("/histograms.json"));
    }

    Y_UNIT_TEST(HistogramsFabric) {
        TMetricRegistry registry(TLabels{{"common", "label"}});
        bool called = false;

        auto collector = [&]() {
            called = true;
            return ExponentialHistogram(5, 2);
        };

        THistogram* h1 = registry.HistogramCounter(
                {{"sensor", "readTimeMillis"}},
                collector);

        UNIT_ASSERT_VALUES_EQUAL(called, true);
        called = false;

        h1 = registry.HistogramCounter(
            {{"sensor", "readTimeMillis"}},
            collector);

        UNIT_ASSERT_VALUES_EQUAL(called, false);

        THistogram* h2 = registry.HistogramRate(
            {{"sensor", "writeTimeMillis"}},
            ExplicitHistogram({1, 5, 15, 20, 25}));

        for (i64 i = 0; i < 100; i++) {
            h1->Record(i);
            h2->Record(i);
        }

        TStringStream ss;
        {
            auto encoder = EncoderJson(&ss, 2);
            registry.Accept(TInstant::Zero(), encoder.Get());
        }
        ss << '\n';

        UNIT_ASSERT_NO_DIFF(ss.Str(), NResource::Find("/histograms.json"));
    }

    Y_UNIT_TEST(StreamingEncoderTest) {
        const TString expected {
            "{\"commonLabels\":{\"common\":\"label\"},"
            "\"sensors\":[{\"kind\":\"GAUGE\",\"labels\":{\"my\":\"gauge\"},\"value\":12.34}]}"
        };

        TMetricRegistry registry(TLabels{{"common", "label"}});

        TGauge* g = registry.Gauge({{"my", "gauge"}});
        g->Set(12.34);

        TStringStream os;
        auto encoder = EncoderJson(&os);
        registry.Accept(TInstant::Zero(), encoder.Get());

        UNIT_ASSERT_STRINGS_EQUAL(os.Str(), expected);
    }

    Y_UNIT_TEST(CreatingSameMetricWithDifferentTypesShouldThrow) {
        TMetricRegistry registry;

        registry.Gauge({{"foo", "bar"}});
        UNIT_ASSERT_EXCEPTION(registry.Counter({{"foo", "bar"}}), yexception);

        registry.HistogramCounter({{"bar", "baz"}}, ExponentialHistogram(5, 2));
        UNIT_ASSERT_EXCEPTION(registry.HistogramRate({{"bar", "baz"}}, ExponentialHistogram(5, 2)), yexception);
    }

    Y_UNIT_TEST(EncodeRegistryWithCommonLabels) {
        TMetricRegistry registry(TLabels{{"common", "label"}});

        TGauge* g = registry.Gauge({{"my", "gauge"}});
        g->Set(12.34);

        // Append() adds common labels to each metric, allowing to combine
        // several metric registries in one resulting blob
        {
            TStringStream os;
            auto encoder = EncoderJson(&os);
            encoder->OnStreamBegin();
            registry.Append(TInstant::Zero(), encoder.Get());
            encoder->OnStreamEnd();

            UNIT_ASSERT_STRINGS_EQUAL(
                    os.Str(),
                    "{\"sensors\":[{\"kind\":\"GAUGE\",\"labels\":{\"common\":\"label\",\"my\":\"gauge\"},\"value\":12.34}]}");
        }

        // Accept() adds common labels to the beginning of the blob
        {
            TStringStream os;
            auto encoder = EncoderJson(&os);
            registry.Accept(TInstant::Zero(), encoder.Get());

            UNIT_ASSERT_STRINGS_EQUAL(
                    os.Str(),
                    "{\"commonLabels\":{\"common\":\"label\"},"
                    "\"sensors\":[{\"kind\":\"GAUGE\",\"labels\":{\"my\":\"gauge\"},\"value\":12.34}]}");
        }
    }

    Y_UNIT_TEST(MetricsRegistryClear) {
        TMetricRegistry registry;
        registry.Gauge({{"some", "label"}})->Add(1);

        NProto::TSingleSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);
        registry.Accept(TInstant::Now(), encoder.Get());

        UNIT_ASSERT(samples.SamplesSize() == 1);

        samples = {};
        registry.Clear();
        registry.Accept(TInstant::Now(), encoder.Get());

        UNIT_ASSERT(samples.SamplesSize() == 0);
    }

    Y_UNIT_TEST(AssignNewRegistry) {
        TMetricRegistry registry;
        registry.Gauge({{"some", "label"}})->Add(1);

        NProto::TSingleSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);
        registry.Accept(TInstant::Now(), encoder.Get());

        UNIT_ASSERT(samples.CommonLabelsSize() == 0);
        UNIT_ASSERT(samples.SamplesSize() == 1);

        samples = {};
        auto newRegistry = TMetricRegistry{{{"common", "label"}}};
        registry = std::move(newRegistry);

        registry.Accept(TInstant::Now(), encoder.Get());

        const auto& commonLabels = samples.GetCommonLabels();

        UNIT_ASSERT(samples.GetSamples().size() == 0);
        UNIT_ASSERT(commonLabels.size() == 1);
        UNIT_ASSERT(commonLabels[0].GetName() == "common");
        UNIT_ASSERT(commonLabels[0].GetValue() == "label");
    }
}

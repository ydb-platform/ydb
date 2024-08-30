#include "prometheus.h"

#include <library/cpp/monlib/encode/protobuf/protobuf.h>
#include <library/cpp/monlib/metrics/metric_value.h>
#include <library/cpp/monlib/metrics/histogram_snapshot.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TPrometheusEncoderTest) {

    template <typename TFunc>
    TString EncodeToString(TFunc fn) {
        TStringStream ss;
        IMetricEncoderPtr encoder = EncoderPrometheus(&ss);
        fn(encoder.Get());
        return ss.Str();
    }

    ISummaryDoubleSnapshotPtr TestSummaryDouble() {
        return MakeIntrusive<TSummaryDoubleSnapshot>(10.1, -0.45, 0.478, 0.3, 30u);
    }

    Y_UNIT_TEST(Empty) {
        auto result = EncodeToString([](IMetricEncoder* e) {
            e->OnStreamBegin();
            e->OnStreamEnd();
        });
        UNIT_ASSERT_STRINGS_EQUAL(result, "\n");
    }

    Y_UNIT_TEST(DoubleGauge) {
        auto result = EncodeToString([](IMetricEncoder* e) {
            e->OnStreamBegin();
            { // no values
                e->OnMetricBegin(EMetricType::GAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "cpuUsage");
                    e->OnLabelsEnd();
                }
                e->OnMetricEnd();
            }
            { // one value no ts
                e->OnMetricBegin(EMetricType::GAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "diskUsage");
                    e->OnLabel("disk", "sda1");
                    e->OnLabelsEnd();
                }
                e->OnDouble(TInstant::Zero(), 1000);
                e->OnMetricEnd();
            }
            { // one value with ts
                e->OnMetricBegin(EMetricType::GAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "memoryUsage");
                    e->OnLabel("host", "solomon-man-00");
                    e->OnLabel("dc", "man");
                    e->OnLabelsEnd();
                }
                e->OnDouble(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:00Z"), 1000);
                e->OnMetricEnd();
            }
            { // many values
                e->OnMetricBegin(EMetricType::GAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "bytesRx");
                    e->OnLabel("host", "solomon-sas-01");
                    e->OnLabel("dc", "sas");
                    e->OnLabelsEnd();
                }
                e->OnDouble(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:00Z"), 2);
                e->OnDouble(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:05Z"), 4);
                e->OnDouble(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:10Z"), 8);
                e->OnMetricEnd();
            }
            { // already seen metric name
                e->OnMetricBegin(EMetricType::GAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "diskUsage");
                    e->OnLabel("disk", "sdb1");
                    e->OnLabelsEnd();
                }
                e->OnDouble(TInstant::Zero(), 1001);
                e->OnMetricEnd();
            }
            { // NaN
                e->OnMetricBegin(EMetricType::GAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "nanValue");
                    e->OnLabelsEnd();
                }
                e->OnDouble(TInstant::Zero(), NAN);
                e->OnMetricEnd();
            }
            { // Inf
                e->OnMetricBegin(EMetricType::GAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "infValue");
                    e->OnLabelsEnd();
                }
                e->OnDouble(TInstant::Zero(), INFINITY);
                e->OnMetricEnd();
            }
            {
                e->OnMetricBegin(EMetricType::DSUMMARY);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "seconds");
                    e->OnLabel("disk", "sdb1");
                    e->OnLabelsEnd();
                }
                e->OnSummaryDouble(TInstant::Zero(), TestSummaryDouble());
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        });

        UNIT_ASSERT_STRINGS_EQUAL(result,
            "# TYPE diskUsage gauge\n"
            "diskUsage{disk=\"sda1\", } 1000\n"
            "# TYPE memoryUsage gauge\n"
            "memoryUsage{host=\"solomon-man-00\", dc=\"man\", } 1000 1512216000000\n"
            "# TYPE bytesRx gauge\n"
            "bytesRx{host=\"solomon-sas-01\", dc=\"sas\", } 8 1512216010000\n"
            "diskUsage{disk=\"sdb1\", } 1001\n"
            "# TYPE nanValue gauge\n"
            "nanValue nan\n"
            "# TYPE infValue gauge\n"
            "infValue inf\n"
            "seconds_sum{disk=\"sdb1\", } 10.1\n"
            "seconds_min{disk=\"sdb1\", } -0.45\n"
            "seconds_max{disk=\"sdb1\", } 0.478\n"
            "seconds_last{disk=\"sdb1\", } 0.3\n"
            "seconds_count{disk=\"sdb1\", } 30\n"
            "\n");
    }

    Y_UNIT_TEST(IntGauges) {
        auto result = EncodeToString([](IMetricEncoder* e) {
            e->OnStreamBegin();
            { // no values
                e->OnMetricBegin(EMetricType::IGAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "cpuUsage");
                    e->OnLabelsEnd();
                }
                e->OnMetricEnd();
            }
            { // one value no ts
                e->OnMetricBegin(EMetricType::IGAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "diskUsage");
                    e->OnLabel("disk", "sda1");
                    e->OnLabelsEnd();
                }
                e->OnInt64(TInstant::Zero(), 1000);
                e->OnMetricEnd();
            }
            { // one value with ts
                e->OnMetricBegin(EMetricType::IGAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "memoryUsage");
                    e->OnLabel("dc", "man");
                    e->OnLabel("host", "solomon-man-00");
                    e->OnLabelsEnd();
                }
                e->OnInt64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:00Z"), 1000);
                e->OnMetricEnd();
            }
            { // many values
                e->OnMetricBegin(EMetricType::IGAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "bytesRx");
                    e->OnLabel("dc", "sas");
                    e->OnLabel("host", "solomon-sas-01");
                    e->OnLabelsEnd();
                }
                e->OnInt64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:00Z"), 2);
                e->OnInt64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:05Z"), 4);
                e->OnInt64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:10Z"), 8);
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        });

        UNIT_ASSERT_STRINGS_EQUAL(result,
            "# TYPE diskUsage gauge\n"
            "diskUsage{disk=\"sda1\", } 1000\n"
            "# TYPE memoryUsage gauge\n"
            "memoryUsage{dc=\"man\", host=\"solomon-man-00\", } 1000 1512216000000\n"
            "# TYPE bytesRx gauge\n"
            "bytesRx{dc=\"sas\", host=\"solomon-sas-01\", } 8 1512216010000\n"
            "\n");
    }

    Y_UNIT_TEST(Counters) {
        auto result = EncodeToString([](IMetricEncoder* e) {
            e->OnStreamBegin();
            { // no values
                e->OnMetricBegin(EMetricType::COUNTER);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "cpuUsage");
                    e->OnLabelsEnd();
                }
                e->OnMetricEnd();
            }
            { // one value no ts
                e->OnMetricBegin(EMetricType::COUNTER);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "diskUsage");
                    e->OnLabel("disk", "sda1");
                    e->OnLabelsEnd();
                }
                e->OnInt64(TInstant::Zero(), 1000);
                e->OnMetricEnd();
            }
            { // one value with ts
                e->OnMetricBegin(EMetricType::COUNTER);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "memoryUsage");
                    e->OnLabel("host", "solomon-man-00");
                    e->OnLabel("dc", "man");
                    e->OnLabelsEnd();
                }
                e->OnInt64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:00Z"), 1000);
                e->OnMetricEnd();
            }
            { // many values
                e->OnMetricBegin(EMetricType::COUNTER);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "bytesRx");
                    e->OnLabel("host", "solomon-sas-01");
                    e->OnLabel("dc", "sas");
                    e->OnLabelsEnd();
                }
                e->OnInt64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:00Z"), 2);
                e->OnInt64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:05Z"), 4);
                e->OnInt64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:10Z"), 8);
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        });

        UNIT_ASSERT_STRINGS_EQUAL(result,
            "# TYPE diskUsage counter\n"
            "diskUsage{disk=\"sda1\", } 1000\n"
            "# TYPE memoryUsage counter\n"
            "memoryUsage{host=\"solomon-man-00\", dc=\"man\", } 1000 1512216000000\n"
            "# TYPE bytesRx counter\n"
            "bytesRx{host=\"solomon-sas-01\", dc=\"sas\", } 8 1512216010000\n"
            "\n");
    }

    Y_UNIT_TEST(Histograms) {
        auto result = EncodeToString([](IMetricEncoder* e) {
            e->OnStreamBegin();
            { // no values histogram
                e->OnMetricBegin(EMetricType::HIST);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "cpuUsage");
                    e->OnLabelsEnd();
                }
                e->OnMetricEnd();
            }
            { // one value no ts
                e->OnMetricBegin(EMetricType::HIST);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "inboundBytesPerSec");
                    e->OnLabel("client", "mbus");
                    e->OnLabelsEnd();
                }
                e->OnHistogram(
                    TInstant::Zero(),
                    ExplicitHistogramSnapshot({10, 20, HISTOGRAM_INF_BOUND}, {1, 4, 0}));
                e->OnMetricEnd();
            }
            { // one value no ts no +inf bucket
                e->OnMetricBegin(EMetricType::HIST);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "inboundBytesPerSec");
                    e->OnLabel("client", "grpc");
                    e->OnLabelsEnd();
                }
                e->OnHistogram(
                    TInstant::Zero(),
                    ExplicitHistogramSnapshot({10, 20, 30}, {1, 4, 0}));
                e->OnMetricEnd();
            }
            { // one value with ts
                e->OnMetricBegin(EMetricType::HIST_RATE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "outboundBytesPerSec");
                    e->OnLabel("client", "grps");
                    e->OnLabelsEnd();
                }
                e->OnHistogram(
                    TInstant::ParseIso8601Deprecated("2017-12-02T12:00:00Z"),
                    ExplicitHistogramSnapshot({100, 200, HISTOGRAM_INF_BOUND}, {1, 0, 0}));
                e->OnMetricEnd();
            }
            { // many values
                e->OnMetricBegin(EMetricType::HIST);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "bytesRx");
                    e->OnLabel("host", "solomon-sas-01");
                    e->OnLabel("dc", "sas");
                    e->OnLabelsEnd();
                }
                TBucketBounds bounds = {100, 200, HISTOGRAM_INF_BOUND};
                e->OnHistogram(
                    TInstant::ParseIso8601Deprecated("2017-12-02T12:00:00Z"),
                    ExplicitHistogramSnapshot(bounds, {10, 0, 0}));
                e->OnHistogram(
                    TInstant::ParseIso8601Deprecated("2017-12-02T12:00:05Z"),
                    ExplicitHistogramSnapshot(bounds, {10, 2, 0}));
                e->OnHistogram(
                    TInstant::ParseIso8601Deprecated("2017-12-02T12:00:10Z"),
                    ExplicitHistogramSnapshot(bounds, {10, 2, 5}));
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        });

        UNIT_ASSERT_STRINGS_EQUAL(result,
            "# TYPE inboundBytesPerSec histogram\n"
            "inboundBytesPerSec_bucket{client=\"mbus\", le=\"10\"} 1\n"
            "inboundBytesPerSec_bucket{client=\"mbus\", le=\"20\"} 5\n"
            "inboundBytesPerSec_bucket{client=\"mbus\", le=\"+Inf\"} 5\n"
            "inboundBytesPerSec_count{client=\"mbus\", } 5\n"
            "inboundBytesPerSec_bucket{client=\"grpc\", le=\"10\"} 1\n"
            "inboundBytesPerSec_bucket{client=\"grpc\", le=\"20\"} 5\n"
            "inboundBytesPerSec_bucket{client=\"grpc\", le=\"30\"} 5\n"
            "inboundBytesPerSec_count{client=\"grpc\", } 5\n"
            "# TYPE outboundBytesPerSec histogram\n"
            "outboundBytesPerSec_bucket{client=\"grps\", le=\"100\"} 1 1512216000000\n"
            "outboundBytesPerSec_bucket{client=\"grps\", le=\"200\"} 1 1512216000000\n"
            "outboundBytesPerSec_bucket{client=\"grps\", le=\"+Inf\"} 1 1512216000000\n"
            "outboundBytesPerSec_count{client=\"grps\", } 1 1512216000000\n"
            "# TYPE bytesRx histogram\n"
            "bytesRx_bucket{host=\"solomon-sas-01\", dc=\"sas\", le=\"100\"} 10 1512216010000\n"
            "bytesRx_bucket{host=\"solomon-sas-01\", dc=\"sas\", le=\"200\"} 12 1512216010000\n"
            "bytesRx_bucket{host=\"solomon-sas-01\", dc=\"sas\", le=\"+Inf\"} 17 1512216010000\n"
            "bytesRx_count{host=\"solomon-sas-01\", dc=\"sas\", } 17 1512216010000\n"
            "\n");
    }

    Y_UNIT_TEST(CommonLables) {
        auto result = EncodeToString([](IMetricEncoder* e) {
            e->OnStreamBegin();
            { // common time
                e->OnCommonTime(TInstant::Seconds(1500000000));
            }
            { // common labels
                e->OnLabelsBegin();
                e->OnLabel("project", "solomon");
                e->OnLabelsEnd();
            }
            { // metric #1
                e->OnMetricBegin(EMetricType::COUNTER);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "single");
                    e->OnLabel("labels", "l1");
                    e->OnLabelsEnd();
                }
                e->OnUint64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:10Z"), 17);
                e->OnMetricEnd();
            }
            { // metric #2
                e->OnMetricBegin(EMetricType::COUNTER);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "two");
                    e->OnLabel("labels", "l2");
                    e->OnLabelsEnd();
                }
                e->OnUint64(TInstant::Zero(), 42);
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        });
    UNIT_ASSERT_STRINGS_EQUAL(result,
R"(# TYPE single counter
single{labels="l1", project="solomon", } 17 1512216010000
# TYPE two counter
two{labels="l2", project="solomon", } 42 1500000000000

)");
    }
}

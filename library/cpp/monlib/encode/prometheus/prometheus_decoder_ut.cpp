#include "prometheus.h"

#include <library/cpp/monlib/encode/protobuf/protobuf.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

#define ASSERT_LABEL_EQUAL(label, name, value) do { \
        UNIT_ASSERT_STRINGS_EQUAL((label).GetName(), name); \
        UNIT_ASSERT_STRINGS_EQUAL((label).GetValue(), value); \
    } while (false)

#define ASSERT_DOUBLE_POINT(s, time, value) do { \
        UNIT_ASSERT_VALUES_EQUAL((s).GetTime(), (time).MilliSeconds()); \
        UNIT_ASSERT_EQUAL((s).GetValueCase(), NProto::TSingleSample::kFloat64); \
        UNIT_ASSERT_DOUBLES_EQUAL((s).GetFloat64(), value, std::numeric_limits<double>::epsilon()); \
    } while (false)

#define ASSERT_UINT_POINT(s, time, value) do { \
        UNIT_ASSERT_VALUES_EQUAL((s).GetTime(), (time).MilliSeconds()); \
        UNIT_ASSERT_EQUAL((s).GetValueCase(), NProto::TSingleSample::kUint64); \
        UNIT_ASSERT_VALUES_EQUAL((s).GetUint64(), value); \
    } while (false)

#define ASSERT_HIST_POINT(s, time, expected) do { \
        UNIT_ASSERT_VALUES_EQUAL((s).GetTime(), time.MilliSeconds()); \
        UNIT_ASSERT_EQUAL((s).GetValueCase(), NProto::TSingleSample::kHistogram);\
        UNIT_ASSERT_VALUES_EQUAL((s).GetHistogram().BoundsSize(), (expected).Count()); \
        UNIT_ASSERT_VALUES_EQUAL((s).GetHistogram().ValuesSize(), (expected).Count()); \
        for (size_t i = 0; i < (s).GetHistogram().BoundsSize(); i++) { \
            UNIT_ASSERT_DOUBLES_EQUAL((s).GetHistogram().GetBounds(i), (expected).UpperBound(i), Min<double>()); \
            UNIT_ASSERT_VALUES_EQUAL((s).GetHistogram().GetValues(i), (expected).Value(i)); \
        } \
    } while (false)

Y_UNIT_TEST_SUITE(TPrometheusDecoderTest) {

    NProto::TSingleSamplesList Decode(TStringBuf data, const TPrometheusDecodeSettings& settings = TPrometheusDecodeSettings{}) {
        NProto::TSingleSamplesList samples;
        ;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);
            DecodePrometheus(data, e.Get(), "sensor", settings);
        }
        return samples;
    }

    Y_UNIT_TEST(Empty) {
        {
            auto samples = Decode("");
            UNIT_ASSERT_EQUAL(samples.SamplesSize(), 0);
        }
        {
            auto samples = Decode("\n");
            UNIT_ASSERT_EQUAL(samples.SamplesSize(), 0);
        }
        {
            auto samples = Decode("\n \n \n");
            UNIT_ASSERT_EQUAL(samples.SamplesSize(), 0);
        }
        {
            auto samples = Decode("\t\n\t\n");
            UNIT_ASSERT_EQUAL(samples.SamplesSize(), 0);
        }
    }

    Y_UNIT_TEST(Minimal) {
        auto samples = Decode(
                "minimal_metric 1.234\n"
                "another_metric -3e3 103948\n"
                "# Even that:\n"
                "no_labels{} 3\n"
                "# HELP line for non-existing metric will be ignored.\n");

        UNIT_ASSERT_EQUAL(samples.SamplesSize(), 3);
        {
            auto& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(1, s.LabelsSize());
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "minimal_metric");
            ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 1.234);
        }
        {
            auto& s = samples.GetSamples(1);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 1);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "another_metric");
            ASSERT_DOUBLE_POINT(s, TInstant::MilliSeconds(103948), -3000.0);
        }
        {
            auto& s = samples.GetSamples(2);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(1, s.LabelsSize());
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "no_labels");
            ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 3.0);
        }
    }

    Y_UNIT_TEST(Counter) {
        constexpr auto inputMetrics =
            "# A normal comment.\n"
            "#\n"
            "# TYPE name counter\n"
            "name{labelname=\"val1\",basename=\"basevalue\"} NaN\n"
            "name {labelname=\"val2\",basename=\"basevalue\"} 2.3 1234567890\n"
            "# HELP name two-line\\n doc  str\\\\ing\n";

        {
            auto samples = Decode(inputMetrics);
            UNIT_ASSERT_EQUAL(samples.SamplesSize(), 2);

            {
                auto& s = samples.GetSamples(0);
                UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
                UNIT_ASSERT_EQUAL(s.LabelsSize(), 3);
                ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "name");
                ASSERT_LABEL_EQUAL(s.GetLabels(1), "basename", "basevalue");
                ASSERT_LABEL_EQUAL(s.GetLabels(2), "labelname", "val1");
                ASSERT_UINT_POINT(s, TInstant::Zero(), ui64(0));
            }
            {
                auto& s = samples.GetSamples(1);
                UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
                UNIT_ASSERT_EQUAL(s.LabelsSize(), 3);
                ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "name");
                ASSERT_LABEL_EQUAL(s.GetLabels(1), "basename", "basevalue");
                ASSERT_LABEL_EQUAL(s.GetLabels(2), "labelname", "val2");
                ASSERT_UINT_POINT(s, TInstant::MilliSeconds(1234567890), i64(2));
            }
        }
        {
            TPrometheusDecodeSettings settings;
            settings.Mode = EPrometheusDecodeMode::RAW;
            auto samples = Decode(inputMetrics, settings);
            UNIT_ASSERT_EQUAL(samples.SamplesSize(), 2);

            {
                auto& s = samples.GetSamples(0);
                UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
                UNIT_ASSERT_EQUAL(s.LabelsSize(), 3);
                ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "name");
                ASSERT_LABEL_EQUAL(s.GetLabels(1), "basename", "basevalue");
                ASSERT_LABEL_EQUAL(s.GetLabels(2), "labelname", "val1");
                ASSERT_DOUBLE_POINT(s, TInstant::MilliSeconds(0), NAN);
            }
            {
                auto& s = samples.GetSamples(1);
                UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
                UNIT_ASSERT_EQUAL(s.LabelsSize(), 3);
                ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "name");
                ASSERT_LABEL_EQUAL(s.GetLabels(1), "basename", "basevalue");
                ASSERT_LABEL_EQUAL(s.GetLabels(2), "labelname", "val2");
                ASSERT_DOUBLE_POINT(s, TInstant::MilliSeconds(1234567890), 2.3);
            }
        }
    }

    Y_UNIT_TEST(Gauge) {
        auto samples = Decode(
                "# A normal comment.\n"
                "#\n"
                " # HELP  name2  \tdoc str\"ing 2\n"
                "  #    TYPE    name2 gauge\n"
                "name2{labelname=\"val2\"\t,basename   =   \"basevalue2\"\t\t} +Inf 54321\n"
                "name2{ labelname = \"val1\" , }-Inf\n");

        UNIT_ASSERT_EQUAL(samples.SamplesSize(), 2);

        {
            auto& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 3);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "name2");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "basename", "basevalue2");
            ASSERT_LABEL_EQUAL(s.GetLabels(2), "labelname", "val2");
            ASSERT_DOUBLE_POINT(s, TInstant::MilliSeconds(54321), INFINITY);
        }
        {
            auto& s = samples.GetSamples(1);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "name2");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "labelname", "val1");
            ASSERT_DOUBLE_POINT(s, TInstant::Zero(), -INFINITY);
        }
    }

    Y_UNIT_TEST(Summary) {
        auto samples = Decode(
                "# HELP \n"
                "# TYPE my_summary summary\n"
                "my_summary{n1=\"val1\",quantile=\"0.5\"} 110\n"
                "my_summary{n1=\"val1\",quantile=\"0.9\"} 140 1\n"
                "my_summary_count{n1=\"val1\"} 42\n"
                "my_summary_sum{n1=\"val1\"} 08 15\n"
                "# some\n"
                "# funny comments\n"
                "# HELP\n"
                "# HELP my_summary\n"
                "# HELP my_summary \n");

        UNIT_ASSERT_EQUAL(samples.SamplesSize(), 4);

        {
            auto& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 3);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "my_summary");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "quantile", "0.5");
            ASSERT_LABEL_EQUAL(s.GetLabels(2), "n1", "val1");
            ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 110.0);
        }
        {
            auto& s = samples.GetSamples(1);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 3);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "my_summary");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "quantile", "0.9");
            ASSERT_LABEL_EQUAL(s.GetLabels(2), "n1", "val1");
            ASSERT_DOUBLE_POINT(s, TInstant::MilliSeconds(1), 140.0);
        }
        {
            auto& s = samples.GetSamples(2);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "my_summary_count");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "n1", "val1");
            ASSERT_UINT_POINT(s, TInstant::Zero(), 42);
        }
        {
            auto& s = samples.GetSamples(3);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "my_summary_sum");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "n1", "val1");
            ASSERT_DOUBLE_POINT(s, TInstant::MilliSeconds(15), 8.0);
        }
    }

    Y_UNIT_TEST(Histogram) {
        constexpr auto inputMetrics =
            "# HELP request_duration_microseconds The response latency.\n"
            "# TYPE request_duration_microseconds histogram\n"
            "request_duration_microseconds_bucket{le=\"0\"} 0\n"
            "request_duration_microseconds_bucket{le=\"100\"} 123\n"
            "request_duration_microseconds_bucket{le=\"120\"} 412\n"
            "request_duration_microseconds_bucket{le=\"144\"} 592\n"
            "request_duration_microseconds_bucket{le=\"172.8\"} 1524\n"
            "request_duration_microseconds_bucket{le=\"+Inf\"} 2693\n"
            "request_duration_microseconds_sum 1.7560473e+06\n"
            "request_duration_microseconds_count 2693\n";

        {
            auto samples = Decode(inputMetrics);
            {
                auto& s = samples.GetSamples(0);
                UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
                UNIT_ASSERT_EQUAL(s.LabelsSize(), 1);
                ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "request_duration_microseconds_sum");
                ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 1756047.3);
            }
            {
                auto& s = samples.GetSamples(1);
                UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
                UNIT_ASSERT_EQUAL(s.LabelsSize(), 1);
                ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "request_duration_microseconds_count");
                ASSERT_UINT_POINT(s, TInstant::Zero(), 2693);
            }
            {
                auto& s = samples.GetSamples(2);
                UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::HIST_RATE);
                UNIT_ASSERT_EQUAL(s.LabelsSize(), 1);
                ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "request_duration_microseconds");
                auto hist = ExplicitHistogramSnapshot(
                    {0, 100, 120, 144, 172.8, HISTOGRAM_INF_BOUND},
                    {0, 123, 289, 180, 932, 1169});
                ASSERT_HIST_POINT(s, TInstant::Zero(), *hist);
            }
        }
        {
            TPrometheusDecodeSettings settings;
            settings.Mode = EPrometheusDecodeMode::RAW;
            auto samples = Decode(inputMetrics, settings);
            UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 8);
            {
                auto& s = samples.GetSamples(0);
                UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
                UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
                ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "request_duration_microseconds_bucket");
                ASSERT_LABEL_EQUAL(s.GetLabels(1), "le", "0");
                ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 0);
            }
        }
    }

    Y_UNIT_TEST(HistogramWithLabels) {
        auto samples = Decode(
                "# A histogram, which has a pretty complex representation in the text format:\n"
                "# HELP http_request_duration_seconds A histogram of the request duration.\n"
                "# TYPE http_request_duration_seconds histogram\n"
                "http_request_duration_seconds_bucket{le=\"0.05\", method=\"POST\"} 24054\n"
                "http_request_duration_seconds_bucket{method=\"POST\", le=\"0.1\"} 33444\n"
                "http_request_duration_seconds_bucket{le=\"0.2\", method=\"POST\", } 100392\n"
                "http_request_duration_seconds_bucket{le=\"0.5\",method=\"POST\",} 129389\n"
                "http_request_duration_seconds_bucket{ method=\"POST\", le=\"1\", } 133988\n"
                "http_request_duration_seconds_bucket{ le=\"+Inf\", method=\"POST\", } 144320\n"
                "http_request_duration_seconds_sum{method=\"POST\"} 53423\n"
                "http_request_duration_seconds_count{ method=\"POST\", } 144320\n");

        UNIT_ASSERT_EQUAL(samples.SamplesSize(), 3);

        {
            auto& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "http_request_duration_seconds_sum");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "method", "POST");
            ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 53423.0);
        }
        {
            auto& s = samples.GetSamples(1);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "http_request_duration_seconds_count");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "method", "POST");
            ASSERT_UINT_POINT(s, TInstant::Zero(), 144320);
        }
        {
            auto& s = samples.GetSamples(2);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::HIST_RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "http_request_duration_seconds");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "method", "POST");
            auto hist = ExplicitHistogramSnapshot(
                    { 0.05, 0.1, 0.2, 0.5, 1, HISTOGRAM_INF_BOUND },
                    { 24054, 9390, 66948, 28997, 4599, 10332 });
            ASSERT_HIST_POINT(s, TInstant::Zero(), *hist);
        }
    }

    Y_UNIT_TEST(MultipleHistograms) {
        auto samples = Decode(
                "# TYPE inboundBytesPerSec histogram\n"
                "inboundBytesPerSec_bucket{client=\"mbus\", le=\"10.0\"} 1.0\n"
                "inboundBytesPerSec_bucket{client=\"mbus\", le=\"20.0\"} 5.0\n"
                "inboundBytesPerSec_bucket{client=\"mbus\", le=\"+Inf\"} 5.0\n"
                "inboundBytesPerSec_count{client=\"mbus\"} 5.0\n"
                "inboundBytesPerSec_bucket{client=\"grpc\", le=\"10.0\"} 1.0\n"
                "inboundBytesPerSec_bucket{client=\"grpc\", le=\"20.0\"} 5.0\n"
                "inboundBytesPerSec_bucket{client=\"grpc\", le=\"30.0\"} 5.0\n"
                "inboundBytesPerSec_count{client=\"grpc\"} 5.0\n"
                "# TYPE outboundBytesPerSec histogram\n"
                "outboundBytesPerSec_bucket{client=\"grpc\", le=\"100.0\"} 1.0 1512216000000\n"
                "outboundBytesPerSec_bucket{client=\"grpc\", le=\"200.0\"} 1.0 1512216000000\n"
                "outboundBytesPerSec_bucket{client=\"grpc\", le=\"+Inf\"} 1.0 1512216000000\n"
                "outboundBytesPerSec_count{client=\"grpc\"} 1.0 1512216000000\n");

        UNIT_ASSERT_EQUAL(samples.SamplesSize(), 6);

        {
            auto& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "inboundBytesPerSec_count");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "client", "mbus");
            ASSERT_UINT_POINT(s, TInstant::Zero(), 5);
        }
        {
            auto& s = samples.GetSamples(1);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::HIST_RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "inboundBytesPerSec");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "client", "mbus");
            auto hist = ExplicitHistogramSnapshot(
                    { 10, 20, HISTOGRAM_INF_BOUND },
                    { 1, 4, 0 });
            ASSERT_HIST_POINT(s, TInstant::Zero(), *hist);
        }
        {
            auto& s = samples.GetSamples(2);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "inboundBytesPerSec_count");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "client", "grpc");
            ASSERT_UINT_POINT(s, TInstant::Zero(), 5);
        }
        {
            auto& s = samples.GetSamples(3);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::HIST_RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "inboundBytesPerSec");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "client", "grpc");
            auto hist = ExplicitHistogramSnapshot(
                    { 10, 20, 30 },
                    { 1, 4, 0 });
            ASSERT_HIST_POINT(s, TInstant::Zero(), *hist);
        }
        {
            auto& s = samples.GetSamples(4);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "outboundBytesPerSec_count");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "client", "grpc");
            ASSERT_UINT_POINT(s, TInstant::Seconds(1512216000), 1) ;
        }
        {
            auto& s = samples.GetSamples(5);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::HIST_RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "outboundBytesPerSec");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "client", "grpc");
            auto hist = ExplicitHistogramSnapshot(
                    { 100, 200, HISTOGRAM_INF_BOUND },
                    { 1, 0, 0 });
            ASSERT_HIST_POINT(s, TInstant::Seconds(1512216000), *hist);
        }
    }

    Y_UNIT_TEST(MixedTypes) {
        auto samples = Decode(
                "# HELP http_requests_total The total number of HTTP requests.\n"
                "# TYPE http_requests_total counter\n"
                "http_requests_total { } 1027 1395066363000\n"
                "http_requests_total{method=\"post\",code=\"200\"} 1027 1395066363000\n"
                "http_requests_total{method=\"post\",code=\"400\"}    3 1395066363000\n"
                "\n"
                "# Minimalistic line:\n"
                "metric_without_timestamp_and_labels 12.47\n"
                "\n"
                "# HELP rpc_duration_seconds A summary of the RPC duration in seconds.\n"
                "# TYPE rpc_duration_seconds summary\n"
                "rpc_duration_seconds{quantile=\"0.01\"} 3102\n"
                "rpc_duration_seconds{quantile=\"0.5\"} 4773\n"
                "rpc_duration_seconds{quantile=\"0.9\"} 9001\n"
                "rpc_duration_seconds_sum 1.7560473e+07\n"
                "rpc_duration_seconds_count 2693\n"
                "\n"
                "# Another mMinimalistic line:\n"
                "metric_with_timestamp 12.47 1234567890\n");

        UNIT_ASSERT_EQUAL(samples.SamplesSize(), 10);

        {
            auto& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 1);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "http_requests_total");
            ASSERT_UINT_POINT(s, TInstant::Seconds(1395066363), 1027);
        }
        {
            auto& s = samples.GetSamples(1);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 3);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "http_requests_total");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "method", "post");
            ASSERT_LABEL_EQUAL(s.GetLabels(2), "code", "200");
            ASSERT_UINT_POINT(s, TInstant::Seconds(1395066363), 1027);
        }
        {
            auto& s = samples.GetSamples(2);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 3);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "http_requests_total");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "method", "post");
            ASSERT_LABEL_EQUAL(s.GetLabels(2), "code", "400");
            ASSERT_UINT_POINT(s, TInstant::Seconds(1395066363), 3);
        }
        {
            auto& s = samples.GetSamples(3);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 1);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "metric_without_timestamp_and_labels");
            ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 12.47);
        }
        {
            auto& s = samples.GetSamples(4);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "rpc_duration_seconds");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "quantile", "0.01");
            ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 3102);
        }
        {
            auto& s = samples.GetSamples(5);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "rpc_duration_seconds");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "quantile", "0.5");
            ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 4773);
        }
        {
            auto& s = samples.GetSamples(6);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 2);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "rpc_duration_seconds");
            ASSERT_LABEL_EQUAL(s.GetLabels(1), "quantile", "0.9");
            ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 9001);
        }
        {
            auto& s = samples.GetSamples(7);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 1);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "rpc_duration_seconds_sum");
            ASSERT_DOUBLE_POINT(s, TInstant::Zero(), 17560473);
        }
        {
            auto& s = samples.GetSamples(8);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::RATE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 1);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "rpc_duration_seconds_count");
            ASSERT_UINT_POINT(s, TInstant::Zero(), 2693);
        }
        {
            auto& s = samples.GetSamples(9);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::EMetricType::GAUGE);
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 1);
            ASSERT_LABEL_EQUAL(s.GetLabels(0), "sensor", "metric_with_timestamp");
            ASSERT_DOUBLE_POINT(s, TInstant::MilliSeconds(1234567890), 12.47);
        }
    }
}

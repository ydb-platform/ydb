#include "json.h"

#include <library/cpp/monlib/encode/protobuf/protobuf.h>
#include <library/cpp/monlib/metrics/labels.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/builder.h>

#include <limits>

using namespace NMonitoring;

namespace NMonitoring {
    bool operator<(const TLabel& lhs, const TLabel& rhs) {
        return lhs.Name() < rhs.Name() ||
            (lhs.Name() == rhs.Name() && lhs.Value() < rhs.Value());
    }
}
namespace {
    void AssertLabels(const NProto::TMultiSample& actual, const TLabels& expected) {
        UNIT_ASSERT_EQUAL(actual.LabelsSize(), expected.Size());

        TSet<TLabel> actualSet;
        TSet<TLabel> expectedSet;
        Transform(expected.begin(), expected.end(), std::inserter(expectedSet, expectedSet.end()), [] (auto&& l) {
            return TLabel{l.Name(), l.Value()};
        });

        const auto& l = actual.GetLabels();
        Transform(std::begin(l), std::end(l), std::inserter(actualSet, std::begin(actualSet)),
                  [](auto&& elem) -> TLabel {
                      return {elem.GetName(), elem.GetValue()};
                  });

        TVector<TLabel> diff;
        SetSymmetricDifference(std::begin(expectedSet), std::end(expectedSet),
                               std::begin(actualSet), std::end(actualSet), std::back_inserter(diff));

        if (diff.size() > 0) {
            for (auto&& l : diff) {
                Cerr << l << Endl;
            }

            UNIT_FAIL("Labels don't match");
        }
    }

    void AssertLabelEqual(const NProto::TLabel& l, TStringBuf name, TStringBuf value) {
        UNIT_ASSERT_STRINGS_EQUAL(l.GetName(), name);
        UNIT_ASSERT_STRINGS_EQUAL(l.GetValue(), value);
    }

    void AssertPointEqual(const NProto::TPoint& p, TInstant time, double value) {
        UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), time.MilliSeconds());
        UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kFloat64);
        UNIT_ASSERT_DOUBLES_EQUAL(p.GetFloat64(), value, std::numeric_limits<double>::epsilon());
    }

    void AssertPointEqualNan(const NProto::TPoint& p, TInstant time) {
        UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), time.MilliSeconds());
        UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kFloat64);
        UNIT_ASSERT(std::isnan(p.GetFloat64()));
    }

    void AssertPointEqualInf(const NProto::TPoint& p, TInstant time, int sign) {
        UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), time.MilliSeconds());
        UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kFloat64);
        UNIT_ASSERT(std::isinf(p.GetFloat64()));
        if (sign < 0) {
            UNIT_ASSERT(p.GetFloat64() < 0);
        }
    }

    void AssertPointEqual(const NProto::TPoint& p, TInstant time, ui64 value) {
        UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), time.MilliSeconds());
        UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kUint64);
        UNIT_ASSERT_VALUES_EQUAL(p.GetUint64(), value);
    }

    void AssertPointEqual(const NProto::TPoint& p, TInstant time, i64 value) {
        UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), time.MilliSeconds());
        UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kInt64);
        UNIT_ASSERT_VALUES_EQUAL(p.GetInt64(), value);
    }

    void AssertPointEqual(const NProto::TPoint& p, TInstant time, const IHistogramSnapshot& expected) {
        UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), time.MilliSeconds());
        UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kHistogram);

        const NProto::THistogram& h = p.GetHistogram();
        UNIT_ASSERT_VALUES_EQUAL(h.BoundsSize(), expected.Count());
        UNIT_ASSERT_VALUES_EQUAL(h.ValuesSize(), expected.Count());

        for (size_t i = 0; i < h.BoundsSize(); i++) {
            UNIT_ASSERT_DOUBLES_EQUAL(h.GetBounds(i), expected.UpperBound(i), Min<double>());
            UNIT_ASSERT_VALUES_EQUAL(h.GetValues(i), expected.Value(i));
        }
    }

    void AssertPointEqual(const NProto::TPoint& p, TInstant time, const TLogHistogramSnapshot& expected) {
        UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), time.MilliSeconds());
        UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kLogHistogram);

        const double eps = 1e-10;
        const NProto::TLogHistogram& h = p.GetLogHistogram();

        UNIT_ASSERT_DOUBLES_EQUAL(h.GetBase(), expected.Base(), eps);
        UNIT_ASSERT_VALUES_EQUAL(h.GetZerosCount(), expected.ZerosCount());
        UNIT_ASSERT_VALUES_EQUAL(h.GetStartPower(), expected.StartPower());
        UNIT_ASSERT_VALUES_EQUAL(h.BucketsSize(), expected.Count());
        for (size_t i = 0; i < expected.Count(); ++i) {
            UNIT_ASSERT_DOUBLES_EQUAL(h.GetBuckets(i), expected.Bucket(i), eps);
        }
    }

    void AssertPointEqual(const NProto::TPoint& p, TInstant time, const ISummaryDoubleSnapshot& expected) {
        UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), time.MilliSeconds());
        UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kSummaryDouble);
        auto actual = p.GetSummaryDouble();
        const double eps = 1e-10;
        UNIT_ASSERT_DOUBLES_EQUAL(actual.GetSum(), expected.GetSum(), eps);
        UNIT_ASSERT_DOUBLES_EQUAL(actual.GetMin(), expected.GetMin(), eps);
        UNIT_ASSERT_DOUBLES_EQUAL(actual.GetMax(), expected.GetMax(), eps);
        UNIT_ASSERT_DOUBLES_EQUAL(actual.GetLast(), expected.GetLast(), eps);
        UNIT_ASSERT_VALUES_EQUAL(actual.GetCount(), expected.GetCount());
    }

} // namespace


Y_UNIT_TEST_SUITE(TJsonTest) {
    const TInstant now = TInstant::ParseIso8601Deprecated("2017-11-05T01:02:03Z");

    Y_UNIT_TEST(Encode) {
        auto check = [](bool cloud, bool buffered, TStringBuf expectedResourceKey) {
            TString json;
            TStringOutput out(json);
            auto e = cloud
                ? (buffered ? BufferedEncoderCloudJson(&out, 2, "metric") : EncoderCloudJson(&out, 2, "metric"))
                : (buffered ? BufferedEncoderJson(&out, 2) : EncoderJson(&out, 2));
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
                    e->OnLabel("metric", "single");
                    e->OnLabel("labels", "l1");
                    e->OnLabelsEnd();
                }
                e->OnUint64(now, 17);
                e->OnMetricEnd();
            }
            { // metric #2
                e->OnMetricBegin(EMetricType::RATE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "single");
                    e->OnLabel("labels", "l2");
                    e->OnLabelsEnd();
                }
                e->OnUint64(now, 17);
                e->OnMetricEnd();
            }
            { // metric #3
                e->OnMetricBegin(EMetricType::GAUGE);
                e->OnDouble(now, 3.14);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "single");
                    e->OnLabel("labels", "l3");
                    e->OnLabelsEnd();
                }
                e->OnMetricEnd();
            }
            { // metric #4
                e->OnMetricBegin(EMetricType::IGAUGE);
                e->OnInt64(now, 42);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "single_igauge");
                    e->OnLabel("labels", "l4");
                    e->OnLabelsEnd();
                }
                e->OnMetricEnd();
            }
            { // metric #5
                e->OnMetricBegin(EMetricType::GAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "multiple");
                    e->OnLabel("labels", "l5");
                    e->OnLabelsEnd();
                }
                e->OnDouble(now, std::numeric_limits<double>::quiet_NaN());
                e->OnDouble(now + TDuration::Seconds(15), std::numeric_limits<double>::infinity());
                e->OnDouble(now + TDuration::Seconds(30), -std::numeric_limits<double>::infinity());
                e->OnMetricEnd();
            }

            { // metric #6
                e->OnMetricBegin(EMetricType::COUNTER);
                e->OnUint64(now, 1337);
                e->OnUint64(now + TDuration::Seconds(15), 1338);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "multiple");
                    e->OnLabel("labels", "l6");
                    e->OnLabelsEnd();
                }
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
            e->Close();
            json += "\n";

            auto parseJson = [] (auto buf) {
                NJson::TJsonValue value;
                NJson::ReadJsonTree(buf, &value, true);
                return value;
            };

            const auto expectedJson = NResource::Find(expectedResourceKey);
            UNIT_ASSERT_EQUAL(parseJson(json), parseJson(expectedJson));
        };

        check(false, false, "/expected.json");
        check(false, true, "/expected_buffered.json");
        check(true, false, "/expected_cloud.json");
        check(true, true, "/expected_cloud_buffered.json");
    }

    TLogHistogramSnapshotPtr TestLogHistogram(ui32 v = 1) {
        TVector<double> buckets{0.5 * v, 0.25 * v, 0.25 * v, 0.5 * v};
        return MakeIntrusive<TLogHistogramSnapshot>(1.5, 1u, 0, std::move(buckets));
    }

    Y_UNIT_TEST(HistogramAndSummaryMetricTypesAreNotSupportedByCloudJson) {
        const TInstant now = TInstant::ParseIso8601Deprecated("2017-11-05T01:02:03Z");

        auto emit = [&](IMetricEncoder* encoder, EMetricType metricType) {
            encoder->OnStreamBegin();
            {
                encoder->OnMetricBegin(metricType);
                {
                    encoder->OnLabelsBegin();
                    encoder->OnLabel("name", "m");
                    encoder->OnLabelsEnd();
                }

                switch (metricType) {
                    case EMetricType::HIST: {
                        auto histogram = ExponentialHistogram(6, 2);
                        encoder->OnHistogram(now, histogram->Snapshot());
                        break;
                    }
                    case EMetricType::LOGHIST: {
                        auto histogram = TestLogHistogram();
                        encoder->OnLogHistogram(now, histogram);
                        break;
                    }
                    case EMetricType::DSUMMARY: {
                        auto summary = MakeIntrusive<TSummaryDoubleSnapshot>(10., -0.5, 0.5, 0.3, 30u);
                        encoder->OnSummaryDouble(now, summary);
                        break;
                    }
                    default:
                        Y_ABORT("unexpected metric type [%s]", ToString(metricType).c_str());
                }

                encoder->OnMetricEnd();
            }
            encoder->OnStreamEnd();
            encoder->Close();
        };

        auto doTest = [&](bool buffered, EMetricType metricType) {
            TString json;
            TStringOutput out(json);
            auto encoder = buffered ? BufferedEncoderCloudJson(&out, 2) : EncoderCloudJson(&out, 2);
            const TString expectedMessage = TStringBuilder()
                << "metric type '" << metricType << "' is not supported by cloud json format";
            UNIT_ASSERT_EXCEPTION_CONTAINS_C(emit(encoder.Get(), metricType), yexception, expectedMessage,
                                             TString("buffered: ") + ToString(buffered));
        };

        doTest(false, EMetricType::HIST);
        doTest(false, EMetricType::LOGHIST);
        doTest(false, EMetricType::DSUMMARY);
        doTest(true, EMetricType::HIST);
        doTest(true, EMetricType::LOGHIST);
        doTest(true, EMetricType::DSUMMARY);
    }

    Y_UNIT_TEST(MetricsWithDifferentLabelOrderGetMerged) {
        TString json;
        TStringOutput out(json);
        auto e = BufferedEncoderJson(&out, 2);

        e->OnStreamBegin();
        {
            e->OnMetricBegin(EMetricType::RATE);
            {
                e->OnLabelsBegin();
                e->OnLabel("metric", "hello");
                e->OnLabel("label", "world");
                e->OnLabelsEnd();
            }
            e->OnUint64(TInstant::Zero(), 0);
            e->OnMetricEnd();
        }
        {
            e->OnMetricBegin(EMetricType::RATE);
            {
                e->OnLabelsBegin();
                e->OnLabel("label", "world");
                e->OnLabel("metric", "hello");
                e->OnLabelsEnd();
            }
            e->OnUint64(TInstant::Zero(), 1);
            e->OnMetricEnd();
        }
        e->OnStreamEnd();
        e->Close();
        json += "\n";

        TString expectedJson = NResource::Find("/merged.json");
        // we cannot be sure regarding the label order in the result,
        // so we'll have to parse the expected value and then compare it with actual

        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr d = EncoderProtobuf(&samples);
        DecodeJson(expectedJson, d.Get());

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 1);
        {
            const NProto::TMultiSample& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::RATE);
            AssertLabels(s, TLabels{{"metric", "hello"}, {"label", "world"}});

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), TInstant::Zero(), ui64(1));
        }
    }
    Y_UNIT_TEST(Decode1) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString testJson = NResource::Find("/expected.json");
            DecodeJson(testJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::MilliSeconds(samples.GetCommonTime()),
            TInstant::Seconds(1500000000));

        UNIT_ASSERT_VALUES_EQUAL(samples.CommonLabelsSize(), 1);
        AssertLabelEqual(samples.GetCommonLabels(0), "project", "solomon");

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 6);
        {
            const NProto::TMultiSample& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::COUNTER);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
            AssertLabelEqual(s.GetLabels(0), "metric", "single");
            AssertLabelEqual(s.GetLabels(1), "labels", "l1");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), now, ui64(17));
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(1);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::RATE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
            AssertLabelEqual(s.GetLabels(0), "metric", "single");
            AssertLabelEqual(s.GetLabels(1), "labels", "l2");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), now, ui64(17));
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(2);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
            AssertLabelEqual(s.GetLabels(0), "metric", "single");
            AssertLabelEqual(s.GetLabels(1), "labels", "l3");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), now, 3.14);
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(3);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::IGAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
            AssertLabelEqual(s.GetLabels(0), "metric", "single_igauge");
            AssertLabelEqual(s.GetLabels(1), "labels", "l4");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), now, i64(42));
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(4);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
            AssertLabelEqual(s.GetLabels(0), "metric", "multiple");
            AssertLabelEqual(s.GetLabels(1), "labels", "l5");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 3);
            AssertPointEqualNan(s.GetPoints(0), now);
            AssertPointEqualInf(s.GetPoints(1), now + TDuration::Seconds(15), 1);
            AssertPointEqualInf(s.GetPoints(2), now + TDuration::Seconds(30), -11);
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(5);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::COUNTER);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
            AssertLabelEqual(s.GetLabels(0), "metric", "multiple");
            AssertLabelEqual(s.GetLabels(1), "labels", "l6");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 2);
            AssertPointEqual(s.GetPoints(0), now, ui64(1337));
            AssertPointEqual(s.GetPoints(1), now + TDuration::Seconds(15), ui64(1338));
        }
    }

    Y_UNIT_TEST(DecodeMetrics) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString metricsJson = NResource::Find("/metrics.json");
            DecodeJson(metricsJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::MilliSeconds(samples.GetCommonTime()),
            TInstant::ParseIso8601Deprecated("2017-08-27T12:34:56Z"));

        UNIT_ASSERT_VALUES_EQUAL(samples.CommonLabelsSize(), 3);
        AssertLabelEqual(samples.GetCommonLabels(0), "project", "solomon");
        AssertLabelEqual(samples.GetCommonLabels(1), "cluster", "man");
        AssertLabelEqual(samples.GetCommonLabels(2), "service", "stockpile");

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 4);
        {
            const NProto::TMultiSample& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "metric", "Memory");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), TInstant::Zero(), 10.0);
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(1);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::RATE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "metric", "UserTime");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), TInstant::Zero(), ui64(1));
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(2);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
            AssertLabelEqual(s.GetLabels(0), "export", "Oxygen");
            AssertLabelEqual(s.GetLabels(1), "metric", "QueueSize");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            auto ts = TInstant::ParseIso8601Deprecated("2017-11-05T12:34:56.000Z");
            AssertPointEqual(s.GetPoints(0), ts, 3.14159);
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(3);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "metric", "Writes");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 2);
            auto ts1 = TInstant::ParseIso8601Deprecated("2017-08-28T12:32:11Z");
            AssertPointEqual(s.GetPoints(0), ts1, -10.0);
            auto ts2 = TInstant::Seconds(1503923187);
            AssertPointEqual(s.GetPoints(1), ts2, 20.0);
        }
    }

    Y_UNIT_TEST(DecodeSensors) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString sensorsJson = NResource::Find("/sensors.json");
            DecodeJson(sensorsJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::MilliSeconds(samples.GetCommonTime()),
            TInstant::ParseIso8601Deprecated("2017-08-27T12:34:56Z"));

        UNIT_ASSERT_VALUES_EQUAL(samples.CommonLabelsSize(), 3);
        AssertLabelEqual(samples.GetCommonLabels(0), "project", "solomon");
        AssertLabelEqual(samples.GetCommonLabels(1), "cluster", "man");
        AssertLabelEqual(samples.GetCommonLabels(2), "service", "stockpile");

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 4);
        {
            const NProto::TMultiSample& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "metric", "Memory");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), TInstant::Zero(), 10.0);
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(1);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::RATE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "metric", "UserTime");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), TInstant::Zero(), ui64(1));
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(2);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
            AssertLabelEqual(s.GetLabels(0), "export", "Oxygen");
            AssertLabelEqual(s.GetLabels(1), "metric", "QueueSize");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            auto ts = TInstant::ParseIso8601Deprecated("2017-11-05T12:34:56.000Z");
            AssertPointEqual(s.GetPoints(0), ts, 3.14159);
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(3);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "metric", "Writes");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 2);
            auto ts1 = TInstant::ParseIso8601Deprecated("2017-08-28T12:32:11Z");
            AssertPointEqual(s.GetPoints(0), ts1, -10.0);
            auto ts2 = TInstant::Seconds(1503923187);
            AssertPointEqual(s.GetPoints(1), ts2, 20.0);
        }
    }

    Y_UNIT_TEST(DecodeToEncoder) {
        auto testJson = NResource::Find("/test_decode_to_encode.json");

        TStringStream Stream_;
        auto encoder = BufferedEncoderJson(&Stream_, 4);
        DecodeJson(testJson, encoder.Get());

        encoder->Close();

        auto val1 = NJson::ReadJsonFastTree(testJson, true);
        auto val2 = NJson::ReadJsonFastTree(Stream_.Str(), true);

        UNIT_ASSERT_VALUES_EQUAL(val1, val2);
    }

    void WriteEmptySeries(const IMetricEncoderPtr& e) {
        e->OnStreamBegin();
        {
            e->OnMetricBegin(EMetricType::COUNTER);
            {
                e->OnLabelsBegin();
                e->OnLabel("foo", "bar");
                e->OnLabelsEnd();
            }
            e->OnMetricEnd();
        }

        e->OnStreamEnd();
        e->Close();
    }

    Y_UNIT_TEST(EncodeEmptySeries) {
        TString json;
        TStringOutput out(json);

        auto e = EncoderJson(&out, 2);
        WriteEmptySeries(e);
        json += "\n";

        TString expectedJson = NResource::Find("/empty_series.json");
        UNIT_ASSERT_NO_DIFF(json, expectedJson);
    }

    void WriteEmptyLabels(IMetricEncoderPtr& e) {
        e->OnStreamBegin();
        e->OnMetricBegin(EMetricType::COUNTER);

        e->OnLabelsBegin();
        UNIT_ASSERT_EXCEPTION(e->OnLabelsEnd(), yexception);
    }

    Y_UNIT_TEST(LabelsCannotBeEmpty) {
        TString json;
        TStringOutput out(json);

        auto e = EncoderJson(&out, 2);
        WriteEmptyLabels(e);
    }

    Y_UNIT_TEST(LabelsCannotBeEmptyBuffered) {
        TString json;
        TStringOutput out(json);

        auto e = BufferedEncoderJson(&out, 2);
        WriteEmptyLabels(e);
    }

    Y_UNIT_TEST(EncodeEmptySeriesBuffered) {
        TString json;
        TStringOutput out(json);

        auto e = BufferedEncoderJson(&out, 2);
        WriteEmptySeries(e);
        json += "\n";

        TString expectedJson = NResource::Find("/empty_series.json");
        UNIT_ASSERT_NO_DIFF(json, expectedJson);
    }

    Y_UNIT_TEST(BufferedEncoderMergesMetrics) {
        TString json;
        TStringOutput out(json);

        auto e = BufferedEncoderJson(&out, 2);
        auto ts = 1;

        auto writeMetric = [&] (const TString& val) {
            e->OnMetricBegin(EMetricType::COUNTER);

            e->OnLabelsBegin();
            e->OnLabel("foo", val);
            e->OnLabelsEnd();
            e->OnUint64(TInstant::Seconds(ts++), 42);

            e->OnMetricEnd();
        };

        e->OnStreamBegin();
        writeMetric("bar");
        writeMetric("bar");
        writeMetric("baz");
        writeMetric("bar");
        e->OnStreamEnd();
        e->Close();

        json += "\n";

        TString expectedJson = NResource::Find("/buffered_test.json");
        UNIT_ASSERT_NO_DIFF(json, expectedJson);
    }

    Y_UNIT_TEST(JsonEncoderDisallowsValuesInTimeseriesWithoutTs) {
        TStringStream out;

        auto e = EncoderJson(&out);
        auto writePreamble = [&] {
            e->OnStreamBegin();
            e->OnMetricBegin(EMetricType::COUNTER);
            e->OnLabelsBegin();
            e->OnLabel("foo", "bar");
            e->OnLabelsEnd();
        };

        // writing two values for a metric in a row will trigger
        // timeseries object construction
        writePreamble();
        e->OnUint64(TInstant::Zero(), 42);
        UNIT_ASSERT_EXCEPTION(e->OnUint64(TInstant::Zero(), 42), yexception);

        e = EncoderJson(&out);
        writePreamble();
        e->OnUint64(TInstant::Zero(), 42);
        UNIT_ASSERT_EXCEPTION(e->OnUint64(TInstant::Now(), 42), yexception);

        e = EncoderJson(&out);
        writePreamble();
        e->OnUint64(TInstant::Now(), 42);
        UNIT_ASSERT_EXCEPTION(e->OnUint64(TInstant::Zero(), 42), yexception);
    }

    Y_UNIT_TEST(BufferedJsonEncoderMergesTimeseriesWithoutTs) {
        TStringStream out;

        {
            auto e = BufferedEncoderJson(&out, 2);
            e->OnStreamBegin();
            e->OnMetricBegin(EMetricType::COUNTER);
            e->OnLabelsBegin();
            e->OnLabel("foo", "bar");
            e->OnLabelsEnd();
            // in buffered mode we are able to find values with same (in this case zero)
            // timestamp and discard duplicates
            e->OnUint64(TInstant::Zero(), 42);
            e->OnUint64(TInstant::Zero(), 43);
            e->OnUint64(TInstant::Zero(), 44);
            e->OnUint64(TInstant::Zero(), 45);
            e->OnMetricEnd();
            e->OnStreamEnd();
        }

        out << "\n";
        UNIT_ASSERT_NO_DIFF(out.Str(), NResource::Find("/buffered_ts_merge.json"));
    }

    template <typename TFactory, typename TConsumer>
    TString EncodeToString(TFactory factory, TConsumer consumer) {
        TStringStream out;
        {
            IMetricEncoderPtr e = factory(&out, 2);
            consumer(e.Get());
        }
        out << '\n';
        return out.Str();
    }

    Y_UNIT_TEST(SummaryValueEncode) {
        auto writeDocument = [](IMetricEncoder* e) {
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::DSUMMARY);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "temperature");
                    e->OnLabelsEnd();
                }

                e->OnSummaryDouble(now, MakeIntrusive<TSummaryDoubleSnapshot>(10., -0.5, 0.5, 0.3, 30u));
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        };

        TString result1 = EncodeToString(EncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result1, NResource::Find("/summary_value.json"));

        TString result2 = EncodeToString(BufferedEncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result2, NResource::Find("/summary_value.json"));
    }

    ISummaryDoubleSnapshotPtr TestInfSummary() {
        return MakeIntrusive<TSummaryDoubleSnapshot>(
                   std::numeric_limits<double>::quiet_NaN(),
                   -std::numeric_limits<double>::infinity(),
                   std::numeric_limits<double>::infinity(),
                   0.3,
                   30u);
    }

    Y_UNIT_TEST(SummaryInfEncode) {
        auto writeDocument = [](IMetricEncoder* e) {
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::DSUMMARY);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "temperature");
                    e->OnLabelsEnd();
                }

                e->OnSummaryDouble(now, TestInfSummary());
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        };

        TString result1 = EncodeToString(EncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result1, NResource::Find("/summary_inf.json"));

        TString result2 = EncodeToString(BufferedEncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result2, NResource::Find("/summary_inf.json"));
    }

    Y_UNIT_TEST(SummaryInfDecode) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString testJson = NResource::Find("/summary_inf.json");
            DecodeJson(testJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, samples.SamplesSize());
        const NProto::TMultiSample& s = samples.GetSamples(0);

        UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::DSUMMARY);
        UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
        AssertLabelEqual(s.GetLabels(0), "metric", "temperature");

        UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);

        auto actual = s.GetPoints(0).GetSummaryDouble();
        UNIT_ASSERT(std::isnan(actual.GetSum()));
        UNIT_ASSERT(actual.GetMin() < 0);
        UNIT_ASSERT(std::isinf(actual.GetMin()));
        UNIT_ASSERT(actual.GetMax() > 0);
        UNIT_ASSERT(std::isinf(actual.GetMax()));
    }

    Y_UNIT_TEST(SummaryValueDecode) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString testJson = NResource::Find("/summary_value.json");
            DecodeJson(testJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, samples.SamplesSize());
        const NProto::TMultiSample& s = samples.GetSamples(0);

        UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::DSUMMARY);
        UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
        AssertLabelEqual(s.GetLabels(0), "metric", "temperature");

        UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);

        auto snapshot = TSummaryDoubleSnapshot(10., -0.5, 0.5, 0.3, 30u);
        AssertPointEqual(s.GetPoints(0), now, snapshot);
    }

    Y_UNIT_TEST(SummaryTimeSeriesEncode) {
        auto writeDocument = [](IMetricEncoder* e) {
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::DSUMMARY);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "temperature");
                    e->OnLabelsEnd();
                }

                TSummaryDoubleCollector summary;
                summary.Collect(0.3);
                summary.Collect(-0.5);
                summary.Collect(1.);

                e->OnSummaryDouble(now, summary.Snapshot());

                summary.Collect(-1.5);
                summary.Collect(0.01);

                e->OnSummaryDouble(now + TDuration::Seconds(15), summary.Snapshot());

                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        };

        TString result1 = EncodeToString(EncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result1, NResource::Find("/summary_timeseries.json"));

        TString result2 = EncodeToString(BufferedEncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result2, NResource::Find("/summary_timeseries.json"));
    }

    Y_UNIT_TEST(SummaryTimeSeriesDecode) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString testJson = NResource::Find("/summary_timeseries.json");
            DecodeJson(testJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, samples.SamplesSize());
        const NProto::TMultiSample& s = samples.GetSamples(0);

        UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::DSUMMARY);
        UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
        AssertLabelEqual(s.GetLabels(0), "metric", "temperature");

        UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 2);

        TSummaryDoubleCollector summary;
        summary.Collect(0.3);
        summary.Collect(-0.5);
        summary.Collect(1.);

        AssertPointEqual(s.GetPoints(0), now, *summary.Snapshot());

        summary.Collect(-1.5);
        summary.Collect(0.01);

        AssertPointEqual(s.GetPoints(1), now + TDuration::Seconds(15), *summary.Snapshot());
    }

    Y_UNIT_TEST(LogHistogramValueEncode) {
        auto writeDocument = [](IMetricEncoder* e) {
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::LOGHIST);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "ms");
                    e->OnLabelsEnd();
                }

                e->OnLogHistogram(now, TestLogHistogram());
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        };

        TString result1 = EncodeToString(EncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result1, NResource::Find("/log_histogram_value.json"));

        TString result2 = EncodeToString(BufferedEncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result2, NResource::Find("/log_histogram_value.json"));
    }

    Y_UNIT_TEST(LogHistogramValueDecode) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString testJson = NResource::Find("/log_histogram_value.json");
            DecodeJson(testJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, samples.SamplesSize());
        const NProto::TMultiSample& s = samples.GetSamples(0);

        UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::LOGHISTOGRAM);
        UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
        AssertLabelEqual(s.GetLabels(0), "metric", "ms");

        UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);

        auto snapshot = TestLogHistogram();
        AssertPointEqual(s.GetPoints(0), now, *snapshot);
    }

    Y_UNIT_TEST(HistogramValueEncode) {
        auto writeDocument = [](IMetricEncoder* e) {
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::HIST);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "responseTimeMillis");
                    e->OnLabelsEnd();
                }

                // {1: 1, 2: 1, 4: 2, 8: 4, 16: 8, inf: 83}
                auto h = ExponentialHistogram(6, 2);
                for (i64 i = 1; i < 100; i++) {
                    h->Collect(i);
                }

                e->OnHistogram(now, h->Snapshot());
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        };

        TString result1 = EncodeToString(EncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result1, NResource::Find("/histogram_value.json"));

        TString result2 = EncodeToString(BufferedEncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result2, NResource::Find("/histogram_value.json"));
    }

    Y_UNIT_TEST(LogHistogramTimeSeriesEncode) {
        auto writeDocument = [](IMetricEncoder* e) {
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::LOGHIST);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "ms");
                    e->OnLabelsEnd();
                }

                e->OnLogHistogram(now, TestLogHistogram(1));;

                e->OnLogHistogram(now + TDuration::Seconds(15), TestLogHistogram(2));

                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        };

        TString result1 = EncodeToString(EncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result1, NResource::Find("/log_histogram_timeseries.json"));

        TString result2 = EncodeToString(BufferedEncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result2, NResource::Find("/log_histogram_timeseries.json"));
    }

    Y_UNIT_TEST(LogHistogramTimeSeriesDecode) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString testJson = NResource::Find("/log_histogram_timeseries.json");
            DecodeJson(testJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, samples.SamplesSize());
        const NProto::TMultiSample& s = samples.GetSamples(0);

        UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::LOGHISTOGRAM);
        UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
        AssertLabelEqual(s.GetLabels(0), "metric", "ms");

        UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 2);

        auto logHist = TestLogHistogram(1);
        AssertPointEqual(s.GetPoints(0), now, *logHist);

        logHist = TestLogHistogram(2);
        AssertPointEqual(s.GetPoints(1), now + TDuration::Seconds(15), *logHist);
    }

    void HistogramValueDecode(const TString& filePath) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString testJson = NResource::Find(filePath);
            DecodeJson(testJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, samples.SamplesSize());
        const NProto::TMultiSample& s = samples.GetSamples(0);

        UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::HISTOGRAM);
        UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
        AssertLabelEqual(s.GetLabels(0), "metric", "responseTimeMillis");

        UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);

        auto h = ExponentialHistogram(6, 2);
        for (i64 i = 1; i < 100; i++) {
            h->Collect(i);
        }

        AssertPointEqual(s.GetPoints(0), now, *h->Snapshot());
    }

    Y_UNIT_TEST(HistogramValueDecode) {
        HistogramValueDecode("/histogram_value.json");
        HistogramValueDecode("/histogram_value_inf_before_bounds.json");
    }

    Y_UNIT_TEST(HistogramTimeSeriesEncode) {
        auto writeDocument = [](IMetricEncoder* e) {
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::HIST_RATE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "responseTimeMillis");
                    e->OnLabelsEnd();
                }

                // {1: 1, 2: 1, 4: 2, 8: 4, 16: 8, inf: 83}
                auto h = ExponentialHistogram(6, 2);
                for (i64 i = 1; i < 100; i++) {
                    h->Collect(i);
                }
                e->OnHistogram(now, h->Snapshot());

                // {1: 2, 2: 2, 4: 4, 8: 8, 16: 16, inf: 166}
                for (i64 i = 1; i < 100; i++) {
                    h->Collect(i);
                }
                e->OnHistogram(now + TDuration::Seconds(15), h->Snapshot());

                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        };

        TString result1 = EncodeToString(EncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result1, NResource::Find("/histogram_timeseries.json"));

        TString result2 = EncodeToString(BufferedEncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result2, NResource::Find("/histogram_timeseries.json"));
    }

    Y_UNIT_TEST(HistogramTimeSeriesDecode) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString testJson = NResource::Find("/histogram_timeseries.json");
            DecodeJson(testJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, samples.SamplesSize());
        const NProto::TMultiSample& s = samples.GetSamples(0);

        UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::HIST_RATE);
        UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
        AssertLabelEqual(s.GetLabels(0), "metric", "responseTimeMillis");

        UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 2);

        auto h = ExponentialHistogram(6, 2);
        for (i64 i = 1; i < 100; i++) {
            h->Collect(i);
        }

        AssertPointEqual(s.GetPoints(0), now, *h->Snapshot());

        for (i64 i = 1; i < 100; i++) {
            h->Collect(i);
        }

        AssertPointEqual(s.GetPoints(1), now + TDuration::Seconds(15), *h->Snapshot());
    }

    Y_UNIT_TEST(IntGaugeEncode) {
        auto writeDocument = [](IMetricEncoder* e) {
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::IGAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("metric", "a");
                    e->OnLabelsEnd();
                }
                e->OnInt64(now, Min<i64>());
                e->OnInt64(now + TDuration::Seconds(1), -1);
                e->OnInt64(now + TDuration::Seconds(2), 0);
                e->OnInt64(now + TDuration::Seconds(3), Max<i64>());
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        };

        TString result1 = EncodeToString(EncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result1, NResource::Find("/int_gauge.json"));

        TString result2 = EncodeToString(BufferedEncoderJson, writeDocument);
        UNIT_ASSERT_NO_DIFF(result2, NResource::Find("/int_gauge.json"));
    }

    Y_UNIT_TEST(InconsistentMetricTypes) {
        auto emitMetrics = [](IMetricEncoder& encoder, const TString& expectedError) {
            encoder.OnMetricBegin(EMetricType::GAUGE);
            {
                encoder.OnLabelsBegin();
                encoder.OnLabel("name", "m");
                encoder.OnLabel("l1", "v1");
                encoder.OnLabel("l2", "v2");
                encoder.OnLabelsEnd();
            }
            encoder.OnDouble(now, 1.0);
            encoder.OnMetricEnd();

            encoder.OnMetricBegin(EMetricType::COUNTER);
            {
                encoder.OnLabelsBegin();
                encoder.OnLabel("name", "m");
                encoder.OnLabel("l1", "v1");
                encoder.OnLabel("l2", "v2");
                encoder.OnLabelsEnd();
            }
            encoder.OnUint64(now, 1);

            UNIT_ASSERT_EXCEPTION_CONTAINS(encoder.OnMetricEnd(),
                yexception,
                expectedError);
        };

        {
            TStringStream out;
            auto encoder = BufferedEncoderJson(&out);

            encoder->OnStreamBegin();
            encoder->OnLabelsBegin();
            encoder->OnLabel("c", "cv");
            encoder->OnLabelsEnd();
            emitMetrics(*encoder,
                "Time series point type mismatch: expected DOUBLE but found UINT64, "
                "labels '{c=cv, l1=v1, l2=v2, name=m}'");
        }

        {
            TStringStream out;
            auto encoder = BufferedEncoderJson(&out);

            encoder->OnStreamBegin();
            encoder->OnLabelsBegin();
            encoder->OnLabel("l1", "v100");
            encoder->OnLabelsEnd();
            emitMetrics(*encoder,
                "Time series point type mismatch: expected DOUBLE but found UINT64, "
                "labels '{l1=v1, l2=v2, name=m}'");
        }
    }

    Y_UNIT_TEST(IntGaugeDecode) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString testJson = NResource::Find("/int_gauge.json");
            DecodeJson(testJson, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, samples.SamplesSize());
        const NProto::TMultiSample& s = samples.GetSamples(0);

        UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::IGAUGE);
        UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
        AssertLabelEqual(s.GetLabels(0), "metric", "a");

        UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 4);
        AssertPointEqual(s.GetPoints(0), now, Min<i64>());
        AssertPointEqual(s.GetPoints(1), now + TDuration::Seconds(1), i64(-1));
        AssertPointEqual(s.GetPoints(2), now + TDuration::Seconds(2), i64(0));
        AssertPointEqual(s.GetPoints(3), now + TDuration::Seconds(3), Max<i64>());
    }

    Y_UNIT_TEST(FuzzerRegression) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            for (auto f : { "/hist_crash.json", "/crash.json" }) {
                TString testJson = NResource::Find(f);
                UNIT_ASSERT_EXCEPTION(DecodeJson(testJson, e.Get()), yexception);
            }
        }
    }

    Y_UNIT_TEST(LegacyNegativeRateThrows) {
        const auto input = R"({
          "sensors": [
                {
                  "mode": "deriv",
                  "value": -1,
                  "labels": { "metric": "SystemTime" }
                },
            }
            ]}")";

        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);
        UNIT_ASSERT_EXCEPTION(DecodeJson(input, e.Get()), yexception);
    }

    Y_UNIT_TEST(DecodeNamedMetrics) {
        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);

            TString metricsJson = NResource::Find("/named_metrics.json");
            DecodeJson(metricsJson, e.Get(), "sensor");
        }

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 2);
        {
            const NProto::TMultiSample& s = samples.GetSamples(0);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "sensor", "Memory");
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(1);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
            AssertLabelEqual(s.GetLabels(0), "sensor", "QueueSize");
            AssertLabelEqual(s.GetLabels(1), "export", "Oxygen");
        }
    }

}

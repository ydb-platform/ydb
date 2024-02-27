#include "unistat.h"

#include <library/cpp/monlib/encode/protobuf/protobuf.h>
#include <library/cpp/monlib/metrics/labels.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TUnistatDecoderTest) {
    Y_UNIT_TEST(MetricNameLabel) {
        constexpr auto input = TStringBuf(R"([["something_axxx", 42]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        DecodeUnistat(input, encoder.Get(), "metric_name_label");

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 1);
        auto sample = samples.GetSamples(0);

        auto label = sample.GetLabels(0);
        UNIT_ASSERT_VALUES_EQUAL(label.GetName(), "metric_name_label");
    }

    Y_UNIT_TEST(ScalarMetric) {
        constexpr auto input = TStringBuf(R"([["something_axxx", 42]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        DecodeUnistat(input, encoder.Get());

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 1);
        auto sample = samples.GetSamples(0);
        UNIT_ASSERT_EQUAL(sample.GetMetricType(), NProto::GAUGE);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);

        auto label = sample.GetLabels(0);
        auto point = sample.GetPoints(0);
        UNIT_ASSERT_VALUES_EQUAL(point.GetFloat64(), 42.);
        UNIT_ASSERT_VALUES_EQUAL(label.GetName(), "sensor");
        UNIT_ASSERT_VALUES_EQUAL(label.GetValue(), "something_axxx");
    }

    Y_UNIT_TEST(OverriddenTags) {
        constexpr auto input = TStringBuf(R"([["ctype=foo;prj=bar;custom_tag=qwe;something_axxx", 42]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        DecodeUnistat(input, encoder.Get());

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 1);
        auto sample = samples.GetSamples(0);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 4);

        const auto& labels = sample.GetLabels();
        TLabels actual;
        for (auto&& l : labels) {
            actual.Add(l.GetName(), l.GetValue());
        }

        TLabels expected{{"ctype", "foo"}, {"prj", "bar"}, {"custom_tag", "qwe"}, {"sensor", "something_axxx"}};

        UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
        for (auto&& l : actual) {
            UNIT_ASSERT(expected.Extract(l.Name())->Value() == l.Value());
        }
    }

    Y_UNIT_TEST(ThrowsOnTopLevelObject) {
        constexpr auto input = TStringBuf(R"({["something_axxx", 42]})");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        UNIT_ASSERT_EXCEPTION(DecodeUnistat(input, encoder.Get()), yexception);
    }

    Y_UNIT_TEST(ThrowsOnUnwrappedMetric) {
        constexpr auto input = TStringBuf(R"(["something_axxx", 42])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        UNIT_ASSERT_EXCEPTION(DecodeUnistat(input, encoder.Get()), yexception);
    }

    Y_UNIT_TEST(HistogramMetric) {
        constexpr auto input = TStringBuf(R"([["something_hgram", [[0, 1], [200, 2], [500, 3]] ]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        DecodeUnistat(input, encoder.Get());

        auto sample = samples.GetSamples(0);
        UNIT_ASSERT_EQUAL(sample.GetMetricType(), NProto::HIST_RATE);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);

        auto label = sample.GetLabels(0);
        const auto point = sample.GetPoints(0);
        const auto histogram = point.GetHistogram();
        const auto size = histogram.BoundsSize();
        UNIT_ASSERT_VALUES_EQUAL(size, 4);

        const TVector<double> expectedBounds {0, 200, 500, std::numeric_limits<double>::max()};
        const TVector<ui64> expectedValues {0, 1, 2, 3};

        for (auto i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(histogram.GetBounds(i), expectedBounds[i]);
            UNIT_ASSERT_VALUES_EQUAL(histogram.GetValues(i), expectedValues[i]);
        }

        UNIT_ASSERT_VALUES_EQUAL(label.GetName(), "sensor");
        UNIT_ASSERT_VALUES_EQUAL(label.GetValue(), "something_hgram");
    }

    Y_UNIT_TEST(AbsoluteHistogram) {
        constexpr auto input = TStringBuf(R"([["something_ahhh", [[0, 1], [200, 2], [500, 3]] ]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        DecodeUnistat(input, encoder.Get());

        auto sample = samples.GetSamples(0);
        UNIT_ASSERT_EQUAL(sample.GetMetricType(), NProto::HISTOGRAM);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);
    }

    Y_UNIT_TEST(LogHistogram) {
        constexpr auto input = TStringBuf(R"([["something_ahhh", [1, 2, 3] ]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        DecodeUnistat(input, encoder.Get());

        auto sample = samples.GetSamples(0);
        UNIT_ASSERT_EQUAL(sample.GetMetricType(), NProto::HISTOGRAM);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);

        auto histogram = sample.GetPoints(0).GetHistogram();
        UNIT_ASSERT_VALUES_EQUAL(histogram.BoundsSize(), 4);
        TVector<double> expectedBounds = {1, 2, 4, std::numeric_limits<double>::max()};
        TVector<ui64> expectedValues = {1, 1, 1, 0};

        for (auto i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(histogram.GetBounds(i), expectedBounds[i]);
            UNIT_ASSERT_VALUES_EQUAL(histogram.GetValues(i), expectedValues[i]);
        }
    }

    Y_UNIT_TEST(LogHistogramOverflow) {
        constexpr auto input = TStringBuf(R"([["something_ahhh", [1125899906842624, 2251799813685248] ]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        DecodeUnistat(input, encoder.Get());

        auto sample = samples.GetSamples(0);
        UNIT_ASSERT_EQUAL(sample.GetMetricType(), NProto::HISTOGRAM);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);

        auto histogram = sample.GetPoints(0).GetHistogram();
        UNIT_ASSERT_VALUES_EQUAL(histogram.BoundsSize(), HISTOGRAM_MAX_BUCKETS_COUNT);

        TVector<double> expectedBounds;
        for (ui64 i = 0; i < 50; ++i) {
            expectedBounds.push_back(std::pow(2, i));
        }
        expectedBounds.push_back(std::numeric_limits<double>::max());

        TVector<ui64> expectedValues;
        for (ui64 i = 0; i < 50; ++i) {
            expectedValues.push_back(0);
        }
        expectedValues.push_back(2);

        for (auto i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(histogram.GetBounds(i), expectedBounds[i]);
            UNIT_ASSERT_VALUES_EQUAL(histogram.GetValues(i), expectedValues[i]);
        }
    }

    Y_UNIT_TEST(LogHistogramSingle) {
        constexpr auto input = TStringBuf(R"([["something_ahhh", 4 ]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        DecodeUnistat(input, encoder.Get());

        auto sample = samples.GetSamples(0);
        UNIT_ASSERT_EQUAL(sample.GetMetricType(), NProto::HISTOGRAM);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);

        auto histogram = sample.GetPoints(0).GetHistogram();
        UNIT_ASSERT_VALUES_EQUAL(histogram.BoundsSize(), 4);
        TVector<double> expectedBounds = {1, 2, 4, std::numeric_limits<double>::max()};
        TVector<ui64> expectedValues = {0, 0, 1, 0};

        for (auto i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(histogram.GetBounds(i), expectedBounds[i]);
            UNIT_ASSERT_VALUES_EQUAL(histogram.GetValues(i), expectedValues[i]);
        }
    }

    Y_UNIT_TEST(LogHistogramInvalid) {
        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        {
            constexpr auto input = TStringBuf(R"([["something_ahhh", [1, 2, [3, 4]] ]])");
            UNIT_ASSERT_EXCEPTION(DecodeUnistat(input, encoder.Get()), yexception);
        }

        {
            constexpr auto input = TStringBuf(R"([["something_ahhh", [[3, 4], 1, 2] ]])");
            UNIT_ASSERT_EXCEPTION(DecodeUnistat(input, encoder.Get()), yexception);
        }

        {
            constexpr auto input = TStringBuf(R"([["something_hgram", 1, 2, 3, 4 ]])");
            UNIT_ASSERT_EXCEPTION(DecodeUnistat(input, encoder.Get()), yexception);
        }
    }


    Y_UNIT_TEST(AllowedMetricNames) {
        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        {
            constexpr auto input = TStringBuf(R"([["a/A-b/c_D/__G_dmmm", [[0, 1], [200, 2], [500, 3]] ]])");
            UNIT_ASSERT_NO_EXCEPTION(DecodeUnistat(input, encoder.Get()));
        }
    }

    Y_UNIT_TEST(DisallowedMetricNames) {
        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        {
            constexpr auto input = TStringBuf(R"([["someth!ng_ahhh", [[0, 1], [200, 2], [500, 3]] ]])");
            UNIT_ASSERT_EXCEPTION(DecodeUnistat(input, encoder.Get()), yexception);
        }

        {
            constexpr auto input = TStringBuf(R"([["foo_a", [[0, 1], [200, 2], [500, 3]] ]])");
            UNIT_ASSERT_EXCEPTION(DecodeUnistat(input, encoder.Get()), yexception);
        }

        {
            constexpr auto input = TStringBuf(R"([["foo_ahhh;tag=value", [[0, 1], [200, 2], [500, 3]] ]])");
            UNIT_ASSERT_EXCEPTION(DecodeUnistat(input, encoder.Get()), yexception);
        }
    }

    Y_UNIT_TEST(MultipleMetrics) {
        constexpr auto input = TStringBuf(R"([["something_axxx", 42], ["some-other_dhhh", 53]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);

        DecodeUnistat(input, encoder.Get());

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 2);
        auto sample = samples.GetSamples(0);
        UNIT_ASSERT_EQUAL(sample.GetMetricType(), NProto::GAUGE);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);

        auto label = sample.GetLabels(0);
        auto point = sample.GetPoints(0);
        UNIT_ASSERT_VALUES_EQUAL(point.GetFloat64(), 42.);
        UNIT_ASSERT_VALUES_EQUAL(label.GetName(), "sensor");
        UNIT_ASSERT_VALUES_EQUAL(label.GetValue(), "something_axxx");

        sample = samples.GetSamples(1);
        UNIT_ASSERT_EQUAL(sample.GetMetricType(), NProto::RATE);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);

        label = sample.GetLabels(0);
        point = sample.GetPoints(0);
        UNIT_ASSERT_VALUES_EQUAL(point.GetUint64(), 53);
        UNIT_ASSERT_VALUES_EQUAL(label.GetName(), "sensor");
        UNIT_ASSERT_VALUES_EQUAL(label.GetValue(), "some-other_dhhh");
    }

    Y_UNIT_TEST(UnderscoreName) {
        constexpr auto input = TStringBuf(R"([["something_anything_dmmm", 42]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);
        DecodeUnistat(input, encoder.Get());

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 1);
        auto sample = samples.GetSamples(0);
        UNIT_ASSERT_EQUAL(sample.GetMetricType(), NProto::RATE);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);

        auto label = sample.GetLabels(0);
        auto point = sample.GetPoints(0);
        UNIT_ASSERT_VALUES_EQUAL(point.GetUint64(), 42);
        UNIT_ASSERT_VALUES_EQUAL(label.GetName(), "sensor");
        UNIT_ASSERT_VALUES_EQUAL(label.GetValue(), "something_anything_dmmm");
    }

    Y_UNIT_TEST(MaxAggr) {
        constexpr auto input = TStringBuf(R"([["something_anything_max", 42]])");

        NProto::TMultiSamplesList samples;
        auto encoder = EncoderProtobuf(&samples);
        DecodeUnistat(input, encoder.Get());

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 1);
        auto sample = samples.GetSamples(0);
        UNIT_ASSERT_EQUAL(sample.GetMetricType(), NProto::GAUGE);
        UNIT_ASSERT_VALUES_EQUAL(sample.PointsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sample.LabelsSize(), 1);

        auto label = sample.GetLabels(0);
        auto point = sample.GetPoints(0);
        UNIT_ASSERT_VALUES_EQUAL(point.GetFloat64(), 42.);
        UNIT_ASSERT_VALUES_EQUAL(label.GetName(), "sensor");
        UNIT_ASSERT_VALUES_EQUAL(label.GetValue(), "something_anything_max");
    }
}

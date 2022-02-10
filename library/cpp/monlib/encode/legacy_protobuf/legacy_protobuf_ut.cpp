#include "legacy_protobuf.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/monlib/encode/legacy_protobuf/ut/test_cases.pb.h>
#include <library/cpp/monlib/encode/legacy_protobuf/protos/metric_meta.pb.h>

#include <library/cpp/monlib/encode/protobuf/protobuf.h>
#include <library/cpp/monlib/encode/text/text.h>
#include <library/cpp/monlib/metrics/labels.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>

using namespace NMonitoring;

TSimple MakeSimpleMessage() {
    TSimple msg;

    msg.SetFoo(1);
    msg.SetBar(2.);
    msg.SetBaz(42.);

    return msg;
}

IMetricEncoderPtr debugPrinter = EncoderText(&Cerr);

namespace NMonitoring {
    inline bool operator<(const TLabel& lhs, const TLabel& rhs) {
        return lhs.Name() < rhs.Name() ||
               (lhs.Name() == rhs.Name() && lhs.Value() < rhs.Value());
    }

}

void SetLabelValue(NMonProto::TExtraLabelMetrics::TValue& val, TString s) {
    val.SetlabelValue(s);
}

void SetLabelValue(NMonProto::TExtraLabelMetrics::TValue& val, ui64 u) {
    val.SetlabelValueUint(u);
}

template <typename T, typename V>
NMonProto::TExtraLabelMetrics MakeExtra(TString labelName, V labelValue, T value, bool isDeriv) {
    NMonProto::TExtraLabelMetrics metric;
    auto* val = metric.Addvalues();

    metric.SetlabelName(labelName);
    SetLabelValue(*val, labelValue);

    if (isDeriv) {
        val->SetlongValue(value);
    } else {
        val->SetdoubleValue(value);
    }

    return metric;
}

void AssertLabels(const TLabels& expected, const NProto::TMultiSample& actual) {
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

void AssertSimpleMessage(const NProto::TMultiSamplesList& samples, TString pathPrefix = "/") {
    UNIT_ASSERT_EQUAL(samples.SamplesSize(), 3);

    THashSet<TString> expectedValues{pathPrefix + "Foo", pathPrefix + "Bar", pathPrefix + "Baz"};

    for (const auto& s : samples.GetSamples()) {
        UNIT_ASSERT_EQUAL(s.LabelsSize(), 1);
        UNIT_ASSERT_EQUAL(s.PointsSize(), 1);

        const auto labelVal = s.GetLabels(0).GetValue();
        UNIT_ASSERT(expectedValues.contains(labelVal)); 

        if (labelVal == pathPrefix + "Foo") {
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_DOUBLES_EQUAL(s.GetPoints(0).GetFloat64(), 1, 1e-6);
        } else if (labelVal == pathPrefix + "Bar") {
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_DOUBLES_EQUAL(s.GetPoints(0).GetFloat64(), 2, 1e-6);
        } else if (labelVal == pathPrefix + "Baz") {
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::RATE);
            UNIT_ASSERT_EQUAL(s.GetPoints(0).GetUint64(), 42);
        }
    }
}

Y_UNIT_TEST_SUITE(TLegacyProtoDecoderTest) {
    Y_UNIT_TEST(SimpleProto) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        auto msg = MakeSimpleMessage();
        DecodeLegacyProto(msg, e.Get());

        AssertSimpleMessage(samples);
    };

    Y_UNIT_TEST(RepeatedProto) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        auto simple = MakeSimpleMessage();
        TRepeated msg;
        msg.AddMessages()->CopyFrom(simple);

        DecodeLegacyProto(msg, e.Get());

        AssertSimpleMessage(samples);
    }

    Y_UNIT_TEST(RepeatedProtoWithPath) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        auto simple = MakeSimpleMessage();
        TRepeatedWithPath msg;
        msg.AddNamespace()->CopyFrom(simple);

        DecodeLegacyProto(msg, e.Get());

        AssertSimpleMessage(samples, "/Namespace/");
    }

    Y_UNIT_TEST(DeepNesting) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        auto simple = MakeSimpleMessage();
        TRepeatedWithPath internal;
        internal.AddNamespace()->CopyFrom(simple);

        TDeepNesting msg;
        msg.MutableNested()->CopyFrom(internal);

        DecodeLegacyProto(msg, e.Get());

        AssertSimpleMessage(samples, "/Namespace/");
    }

    Y_UNIT_TEST(Keys) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        auto simple = MakeSimpleMessage();
        simple.SetLabel("my_label_value");

        TNestedWithKeys msg;
        msg.AddNamespace()->CopyFrom(simple);

        DecodeLegacyProto(msg, e.Get());

        auto i = 0;
        for (const auto& s : samples.GetSamples()) {
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 4);

            bool foundLabel = false;
            bool foundFixed = false;
            bool foundNumbered = false;

            for (const auto& label : s.GetLabels()) {
                if (label.GetName() == "my_label") {
                    foundLabel = true;
                    UNIT_ASSERT_STRINGS_EQUAL(label.GetValue(), "my_label_value");
                } else if (label.GetName() == "fixed_label") {
                    foundFixed = true;
                    UNIT_ASSERT_STRINGS_EQUAL(label.GetValue(), "fixed_value");
                } else if (label.GetName() == "numbered") {
                    foundNumbered = true;
                    UNIT_ASSERT_STRINGS_EQUAL(label.GetValue(), ::ToString(i));
                }
            }

            UNIT_ASSERT(foundLabel);
            UNIT_ASSERT(foundFixed);
            UNIT_ASSERT(foundNumbered);
        }
    }

    Y_UNIT_TEST(NonStringKeys) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        TNonStringKeys msg;
        msg.SetFoo(42);
        msg.SetEnum(ENUM);
        msg.SetInt(43);

        TRepeatedNonStringKeys msgs;
        msgs.AddNested()->CopyFrom(msg);

        DecodeLegacyProto(msgs, e.Get());

        for (const auto& s : samples.GetSamples()) {
            bool foundEnum = false;
            bool foundInt = false;

            for (const auto& label : s.GetLabels()) {
                if (label.GetName() == "enum") {
                    foundEnum = true;
                    UNIT_ASSERT_STRINGS_EQUAL(label.GetValue(), "ENUM");
                } else if (label.GetName() == "int") {
                    foundInt = true;
                    UNIT_ASSERT_STRINGS_EQUAL(label.GetValue(), "43");
                }
            }

            UNIT_ASSERT(foundEnum);
            UNIT_ASSERT(foundInt);

            UNIT_ASSERT_DOUBLES_EQUAL(s.GetPoints(0).GetFloat64(), 42, 1e-6);
        }
    }

    Y_UNIT_TEST(KeysFromNonLeafNodes) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        auto simple = MakeSimpleMessage();
        simple.SetLabel("label_value");

        TRepeatedWithName nested;
        nested.SetName("my_name");
        nested.AddNested()->CopyFrom(simple);

        TKeysFromNonLeaf msg;
        msg.AddNested()->CopyFrom(nested);

        DecodeLegacyProto(msg, e.Get());

        AssertLabels({{"my_label", "label_value"}, {"path", "/Nested/Nested/Foo"}, {"name", "my_name"}}, samples.GetSamples(0));
    }

    Y_UNIT_TEST(SpacesAreGetReplaced) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        auto simple = MakeSimpleMessage();
        simple.SetLabel("my label_value");

        TNestedWithKeys msg;
        msg.AddNamespace()->CopyFrom(simple);

        DecodeLegacyProto(msg, e.Get());

        for (const auto& s : samples.GetSamples()) {
            UNIT_ASSERT_EQUAL(s.LabelsSize(), 4);

            bool foundLabel = false;

            for (const auto& label : s.GetLabels()) {
                if (label.GetName() == "my_label") {
                    foundLabel = true;
                    UNIT_ASSERT_STRINGS_EQUAL(label.GetValue(), "my_label_value");
                }
            }

            UNIT_ASSERT(foundLabel);
        }
    }

    Y_UNIT_TEST(ExtraLabels) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        TExtraLabels msg;
        msg.MutableExtraAsIs()->CopyFrom(MakeExtra("label", "foo", 42, false));
        msg.MutableExtraDeriv()->CopyFrom(MakeExtra("deriv_label", "deriv_foo", 43, true));

        DecodeLegacyProto(msg, e.Get());

        UNIT_ASSERT_EQUAL(samples.SamplesSize(), 2);
        {
            auto s = samples.GetSamples(0);
            AssertLabels({{"label", "foo"}, {"path", "/ExtraAsIs"}}, s);

            UNIT_ASSERT_EQUAL(s.PointsSize(), 1);
            auto point = s.GetPoints(0);
            UNIT_ASSERT_DOUBLES_EQUAL(point.GetFloat64(), 42, 1e-6);
        }

        {
            auto s = samples.GetSamples(1);
            AssertLabels({{"deriv_label", "deriv_foo"}, {"path", "/ExtraDeriv"}}, s);

            UNIT_ASSERT_EQUAL(s.PointsSize(), 1);
            auto point = s.GetPoints(0);
            UNIT_ASSERT_EQUAL(point.GetUint64(), 43);
        }
    }

    Y_UNIT_TEST(NestedExtraLabels) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        TExtraLabels msg;
        auto extra = MakeExtra("label", "foo", 42, false);
        auto* val = extra.Mutablevalues(0);
        {
            auto child = MakeExtra("child1", "label1", 24, true);
            child.Mutablevalues(0)->Settype(NMonProto::EMetricType::RATE);
            val->Addchildren()->CopyFrom(child);
        }

        {
            auto child = MakeExtra("child2", 34, 23, false);
            val->Addchildren()->CopyFrom(child);
        }
        msg.MutableExtraAsIs()->CopyFrom(extra);

        DecodeLegacyProto(msg, e.Get());

        UNIT_ASSERT_EQUAL(samples.SamplesSize(), 3);
        {
            auto s = samples.GetSamples(0);
            AssertLabels({{"label", "foo"}, {"path", "/ExtraAsIs"}}, s);

            UNIT_ASSERT_EQUAL(s.PointsSize(), 1);
            auto point = s.GetPoints(0);
            UNIT_ASSERT_DOUBLES_EQUAL(point.GetFloat64(), 42, 1e-6);
        }

        {
            auto s = samples.GetSamples(1);
            AssertLabels({{"label", "foo"}, {"child1", "label1"}, {"path", "/ExtraAsIs"}}, s);

            UNIT_ASSERT_EQUAL(s.PointsSize(), 1);
            auto point = s.GetPoints(0);
            UNIT_ASSERT_EQUAL(point.GetUint64(), 24);
        }

        {
            auto s = samples.GetSamples(2);
            AssertLabels({{"label", "foo"}, {"child2", "34"}, {"path", "/ExtraAsIs"}}, s);

            UNIT_ASSERT_EQUAL(s.PointsSize(), 1);
            auto point = s.GetPoints(0);
            UNIT_ASSERT_EQUAL(point.GetFloat64(), 23);
        }
    }

    Y_UNIT_TEST(RobotLabels) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        TNamedCounter responses;
        responses.SetName("responses");
        responses.SetCount(42);

        TCrawlerCounters::TStatusCounters statusCounters;
        statusCounters.AddZoraResponses()->CopyFrom(responses);

        TCrawlerCounters::TPolicyCounters policyCounters;
        policyCounters.SetSubComponent("mySubComponent");
        policyCounters.SetName("myComponentName");
        policyCounters.SetZone("myComponentZone");
        policyCounters.MutableStatusCounters()->CopyFrom(statusCounters);

        TCrawlerCounters counters;
        counters.SetComponent("myComponent");
        counters.AddPoliciesCounters()->CopyFrom(policyCounters);

        DecodeLegacyProto(counters, e.Get());
        UNIT_ASSERT_EQUAL(samples.SamplesSize(), 1);
        auto s = samples.GetSamples(0);
        AssertLabels({
            {"SubComponent", "mySubComponent"}, {"Policy", "myComponentName"}, {"Zone", "myComponentZone"},
            {"ZoraResponse", "responses"}, {"path", "/PoliciesCounters/StatusCounters/ZoraResponses/Count"}}, s);
    }

    Y_UNIT_TEST(ZoraLabels) {
        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        TTimeLogHist hist;
        hist.AddBuckets(42);
        hist.AddBuckets(0);

        TKiwiCounters counters;
        counters.MutableTimes()->CopyFrom(hist);

        DecodeLegacyProto(counters, e.Get());

        UNIT_ASSERT_EQUAL(samples.SamplesSize(), 2);

        auto s = samples.GetSamples(0);
        AssertLabels({{"slot", "0"}, {"path", "/Times/Buckets"}}, s);
        UNIT_ASSERT_EQUAL(s.PointsSize(), 1);
        UNIT_ASSERT_EQUAL(s.GetPoints(0).GetUint64(), 42);

        s = samples.GetSamples(1);
        AssertLabels({{"slot", "1"}, {"path", "/Times/Buckets"}}, s);
        UNIT_ASSERT_EQUAL(s.GetPoints(0).GetUint64(), 0);
    }
}

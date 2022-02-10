#include "encode.h"

#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/protobuf/protobuf.h>

#include <library/cpp/monlib/encode/protobuf/protos/samples.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/buffer.h>
#include <util/stream/buffer.h>

namespace NMonitoring {
    struct TTestData: public TDynamicCounters {
        TTestData() {
            auto hostGroup = GetSubgroup("counters", "resources");
            {
                auto cpuCounter = hostGroup->GetNamedCounter("resource", "cpu");
                *cpuCounter = 30;

                auto memGroup = hostGroup->GetSubgroup("resource", "mem");
                auto usedCounter = memGroup->GetCounter("used");
                auto freeCounter = memGroup->GetCounter("free");
                *usedCounter = 100;
                *freeCounter = 28;

                auto netGroup = hostGroup->GetSubgroup("resource", "net");
                auto rxCounter = netGroup->GetCounter("rx", true);
                auto txCounter = netGroup->GetCounter("tx", true);
                *rxCounter = 8;
                *txCounter = 9;
            }

            auto usersCounter = GetNamedCounter("users", "count");
            *usersCounter = 7;

            auto responseTimeMillis = GetHistogram("responseTimeMillis", ExplicitHistogram({1, 5, 10, 15, 20, 100, 200}));
            for (i64 i = 0; i < 400; i++) {
                responseTimeMillis->Collect(i);
            }
        }
    };

    void AssertLabelsEqual(const NProto::TLabel& l, TStringBuf name, TStringBuf value) {
        UNIT_ASSERT_STRINGS_EQUAL(l.GetName(), name);
        UNIT_ASSERT_STRINGS_EQUAL(l.GetValue(), value);
    }

    void AssertResult(const NProto::TSingleSamplesList& samples) {
        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 7);

        {
            auto s = samples.GetSamples(0);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
            AssertLabelsEqual(s.GetLabels(0), "counters", "resources");
            AssertLabelsEqual(s.GetLabels(1), "resource", "cpu");
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_DOUBLES_EQUAL(s.GetFloat64(), 30.0, Min<double>());
        }
        {
            auto s = samples.GetSamples(1);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 3);
            AssertLabelsEqual(s.GetLabels(0), "counters", "resources");
            AssertLabelsEqual(s.GetLabels(1), "resource", "mem");
            AssertLabelsEqual(s.GetLabels(2), "sensor", "free");
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_DOUBLES_EQUAL(s.GetFloat64(), 28.0, Min<double>());
        }
        {
            auto s = samples.GetSamples(2);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 3);
            AssertLabelsEqual(s.GetLabels(0), "counters", "resources");
            AssertLabelsEqual(s.GetLabels(1), "resource", "mem");
            AssertLabelsEqual(s.GetLabels(2), "sensor", "used");
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_DOUBLES_EQUAL(s.GetFloat64(), 100.0, Min<double>());
        }
        {
            auto s = samples.GetSamples(3);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 3);
            AssertLabelsEqual(s.GetLabels(0), "counters", "resources");
            AssertLabelsEqual(s.GetLabels(1), "resource", "net");
            AssertLabelsEqual(s.GetLabels(2), "sensor", "rx");
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::RATE);
            UNIT_ASSERT_VALUES_EQUAL(s.GetUint64(), 8);
        }
        {
            auto s = samples.GetSamples(4);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 3);
            AssertLabelsEqual(s.GetLabels(0), "counters", "resources");
            AssertLabelsEqual(s.GetLabels(1), "resource", "net");
            AssertLabelsEqual(s.GetLabels(2), "sensor", "tx");
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::RATE);
            UNIT_ASSERT_VALUES_EQUAL(s.GetUint64(), 9);
        }
        {
            auto s = samples.GetSamples(5);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelsEqual(s.GetLabels(0), "sensor", "responseTimeMillis");
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::HIST_RATE);

            const NProto::THistogram& h = s.GetHistogram();

            UNIT_ASSERT_EQUAL(h.BoundsSize(), 8);
            UNIT_ASSERT_DOUBLES_EQUAL(h.GetBounds(0), 1, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(h.GetBounds(1), 5, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(h.GetBounds(2), 10, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(h.GetBounds(3), 15, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(h.GetBounds(4), 20, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(h.GetBounds(5), 100, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(h.GetBounds(6), 200, Min<double>());
            UNIT_ASSERT_DOUBLES_EQUAL(h.GetBounds(7), Max<double>(), Min<double>());

            UNIT_ASSERT_EQUAL(h.ValuesSize(), 8);
            UNIT_ASSERT_EQUAL(h.GetValues(0), 2);
            UNIT_ASSERT_EQUAL(h.GetValues(1), 4);
            UNIT_ASSERT_EQUAL(h.GetValues(2), 5);
            UNIT_ASSERT_EQUAL(h.GetValues(3), 5);
            UNIT_ASSERT_EQUAL(h.GetValues(4), 5);
            UNIT_ASSERT_EQUAL(h.GetValues(5), 80);
            UNIT_ASSERT_EQUAL(h.GetValues(6), 100);
            UNIT_ASSERT_EQUAL(h.GetValues(7), 199);
        }
        {
            auto s = samples.GetSamples(6);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelsEqual(s.GetLabels(0), "users", "count");
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_DOUBLES_EQUAL(s.GetFloat64(), 7, Min<double>());
        }
    }

    Y_UNIT_TEST_SUITE(TDynamicCountersEncodeTest) {
        TTestData Data;

        Y_UNIT_TEST(Json) {
            TString result;
            {
                TStringOutput out(result);
                auto encoder = CreateEncoder(&out, EFormat::JSON);
                Data.Accept(TString(), TString(), *encoder);
            }

            NProto::TSingleSamplesList samples;
            {
                auto e = EncoderProtobuf(&samples);
                DecodeJson(result, e.Get());
            }

            AssertResult(samples);
        }

        Y_UNIT_TEST(Spack) {
            TBuffer result;
            {
                TBufferOutput out(result);
                auto encoder = CreateEncoder(&out, EFormat::SPACK);
                Data.Accept(TString(), TString(), *encoder);
            }

            NProto::TSingleSamplesList samples;
            {
                auto e = EncoderProtobuf(&samples);
                TBufferInput in(result);
                DecodeSpackV1(&in, e.Get());
            }

            AssertResult(samples);
        }

        Y_UNIT_TEST(PrivateSubgroupIsNotSerialized) {
            TBuffer result;
            auto subGroup = MakeIntrusive<TDynamicCounters>(TCountableBase::EVisibility::Private);
            subGroup->GetCounter("hello");
            Data.RegisterSubgroup("foo", "bar", subGroup);

            {
                TBufferOutput out(result);
                auto encoder = CreateEncoder(&out, EFormat::SPACK);
                Data.Accept(TString(), TString(), *encoder);
            }

            NProto::TSingleSamplesList samples;
            {
                auto e = EncoderProtobuf(&samples);
                TBufferInput in(result);
                DecodeSpackV1(&in, e.Get());
            }

            AssertResult(samples);
        }

        Y_UNIT_TEST(PrivateCounterIsNotSerialized) {
            TBuffer result;
            Data.GetCounter("foo", false, TCountableBase::EVisibility::Private);

            {
                TBufferOutput out(result);
                auto encoder = CreateEncoder(&out, EFormat::SPACK);
                Data.Accept(TString(), TString(), *encoder);
            }

            NProto::TSingleSamplesList samples;
            {
                auto e = EncoderProtobuf(&samples);
                TBufferInput in(result);
                DecodeSpackV1(&in, e.Get());
            }

            AssertResult(samples);
        }

        Y_UNIT_TEST(ToJson) {
            TString result = ToJson(Data);

            NProto::TSingleSamplesList samples;
            {
                auto e = EncoderProtobuf(&samples);
                DecodeJson(result, e.Get());
            }

            AssertResult(samples);
        }
    }

}

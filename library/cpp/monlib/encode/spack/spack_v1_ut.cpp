#include "spack_v1.h"

#include <library/cpp/monlib/encode/protobuf/protobuf.h>
#include <library/cpp/monlib/metrics/labels.h>
#include <library/cpp/monlib/metrics/metric.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/buffer.h>
#include <util/stream/buffer.h>
#include <util/string/hex.h>

#include <utility>

using namespace NMonitoring;

#define UNIT_ASSERT_BINARY_EQUALS(a, b)                                   \
    do {                                                                  \
        auto size = Y_ARRAY_SIZE(b);                                      \
        if (Y_UNLIKELY(::memcmp(a, b, size) != 0)) {                      \
            auto as = HexEncode(a, size);                                 \
            auto bs = HexEncode(b, size);                                 \
            UNIT_FAIL_IMPL("equal assertion failed " #a " == " #b,        \
                           "\n  actual: " << as << "\nexpected: " << bs); \
        }                                                                 \
    } while (0)

void AssertLabelEqual(const NProto::TLabel& l, TStringBuf name, TStringBuf value) {
    UNIT_ASSERT_STRINGS_EQUAL(l.GetName(), name);
    UNIT_ASSERT_STRINGS_EQUAL(l.GetValue(), value);
}

void AssertPointEqual(const NProto::TPoint& p, TInstant time, double value) {
    UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), time.MilliSeconds());
    UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kFloat64);
    UNIT_ASSERT_DOUBLES_EQUAL(p.GetFloat64(), value, std::numeric_limits<double>::epsilon());
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

Y_UNIT_TEST_SUITE(TSpackTest) {
    ui8 expectedHeader_v1_0[] = {
        0x53, 0x50,             // magic "SP"                     (fixed ui16)
    // minor, major
        0x00, 0x01,             // version                        (fixed ui16)
        0x18, 0x00,             // header size                    (fixed ui16)
        0x00,                   // time precision                 (fixed ui8)
        0x00,                   // compression algorithm          (fixed ui8)
        0x0d, 0x00, 0x00, 0x00, // label names size   (fixed ui32)
        0x40, 0x00, 0x00, 0x00, // labels values size (fixed ui32)
        0x08, 0x00, 0x00, 0x00, // metric count       (fixed ui32)
        0x08, 0x00, 0x00, 0x00, // points count       (fixed ui32)
    };

    ui8 expectedHeader[] = {
        0x53, 0x50,             // magic "SP"                     (fixed ui16)
    // minor, major
        0x01, 0x01,             // version                        (fixed ui16)
        0x18, 0x00,             // header size                    (fixed ui16)
        0x00,                   // time precision                 (fixed ui8)
        0x00,                   // compression algorithm          (fixed ui8)
        0x0d, 0x00, 0x00, 0x00, // label names size   (fixed ui32)
        0x40, 0x00, 0x00, 0x00, // labels values size (fixed ui32)
        0x08, 0x00, 0x00, 0x00, // metric count       (fixed ui32)
        0x08, 0x00, 0x00, 0x00, // points count       (fixed ui32)
    };

    ui8 expectedStringPools[] = {
        0x6e, 0x61, 0x6d, 0x65, 0x00,                   // "name\0"
        0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x00, // "project\0"
        0x73, 0x6f, 0x6c, 0x6f, 0x6d, 0x6f, 0x6e, 0x00, // "solomon\0"
        0x71, 0x31, 0x00,                               // "q1\0"
        0x71, 0x32, 0x00,                               // "q2\0"
        0x71, 0x33, 0x00,                               // "q3\0"
        0x61, 0x6e, 0x73, 0x77, 0x65, 0x72, 0x00,       // "answer\0"
        0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, // "responseTimeMillis\0"
        0x54, 0x69, 0x6d, 0x65, 0x4d, 0x69, 0x6c, 0x6c,
        0x69, 0x73, 0x00,
        0x62, 0x79, 0x74, 0x65, 0x73, 0x00,             // "bytes\0"
        0x74, 0x65, 0x6D, 0x70, 0x65, 0x72, 0x61, 0x74, // "temperature\0"
        0x75, 0x72, 0x65, 0x00,
        0x6d, 0x73, 0x00,                               // "ms\0"
    };

    ui8 expectedCommonTime[] = {
        0x00, 0x2f, 0x68, 0x59, // common time in seconds (fixed ui32)
    };

    ui8 expectedCommonLabels[] = {
        0x01, // common labels count                     (varint)
        0x01, // label name index                        (varint)
        0x00, // label value index                       (varint)
    };

    ui8 expectedMetric1[] = {
        0x0C, // types (RATE | NONE)                     (fixed ui8)
        0x00, // flags                                   (fixed ui8)
        0x01, // metric labels count                     (varint)
        0x00, // label name index                        (varint)
        0x01, // label value index                       (varint)
    };

    ui8 expectedMetric2[] = {
        0x09,                                           // types (COUNTER | ONE_WITHOUT_TS)        (fixed ui8)
        0x00,                                           // flags                                   (fixed ui8)
        0x01,                                           // metric labels count                     (varint)
        0x00,                                           // label name index                        (varint)
        0x02,                                           // label value index                       (varint)
        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value (fixed ui64)
    };

    ui8 expectedMetric3[] = {
        0x0a,                                           // types (COUNTER | ONE_WITH_TS)           (fixed ui8)
        0x00,                                           // flags                                   (fixed ui8)
        0x01,                                           // metric labels count                     (varint)
        0x00,                                           // label name index                        (varint)
        0x03,                                           // label value index                       (varint)
        0x0b, 0x63, 0xfe, 0x59,                         // time in seconds       (fixed ui32)
        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value (fixed ui64)
    };

    ui8 expectedMetric4[] = {
        0x07,                                           // types (GAUGE | MANY_WITH_TS)            (fixed ui8)
        0x00,                                           // flags                                   (fixed ui8)
        0x01,                                           // metric labels count                     (varint)
        0x00,                                           // label name index                        (varint)
        0x04,                                           // label value index                       (varint)
        0x02,                                           // points count                            (varint)
        0x0b, 0x63, 0xfe, 0x59,                         // time in seconds       (fixed ui32)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x45, 0x40, // value (double IEEE754)
        0x1a, 0x63, 0xfe, 0x59,                         // time in seconds       (fixed ui32)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x4d, 0x40  // value (double IEEE754)
    };

    ui8 expectedMetric5_v1_0[] = {
        0x16,                                           // types (HIST | ONE_WITH_TS)              (fixed ui8)
        0x00,                                           // flags                                   (fixed ui8)
        0x01,                                           // metric labels count                     (varint)
        0x00,                                           // label name index                        (varint)
        0x05,                                           // label value index                       (varint)
        0x0b, 0x63, 0xfe, 0x59,                         // time in seconds                         (fixed ui32)
        0x06,                                           // histogram buckets count                 (varint)
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // histogram bucket bounds                 (array of fixed ui64)
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // histogram bucket values
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x53, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    ui8 expectedMetric5[] = {
        0x16,                                           // types (HIST | ONE_WITH_TS)              (fixed ui8)
        0x00,                                           // flags                                   (fixed ui8)
        0x01,                                           // metric labels count                     (varint)
        0x00,                                           // label name index                        (varint)
        0x05,                                           // label value index                       (varint)
        0x0b, 0x63, 0xfe, 0x59,                         // time in seconds                         (fixed ui32)
        0x06,                                           // histogram buckets count                 (varint)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // histogram bucket bounds                 (array of doubles)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x40,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0x7f,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // histogram bucket values
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x53, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    ui8 expectedMetric6[] = {
        0x12,                                           // types (IGAUGE | ONE_WITH_TS)            (fixed ui8)
        0x00,                                           // flags                                   (fixed ui8)
        0x01,                                           // metric labels count                     (varint)
        0x00,                                           // label name index                        (varint)
        0x06,                                           // label value index                       (varint)
        0x0b, 0x63, 0xfe, 0x59,                         // time in seconds                         (fixed ui32)
        0x39, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value                                   (fixed i64)
    };

    ui8 expectedMetric7[] = {
        0x1e,                                           // types (DSUMMARY | ONE_WITH_TS)          (fixed ui8)
        0x00,                                           // flags                                   (fixed ui8)
        0x01,                                           // metric labels count                     (varint)
        0x00,                                           // label name index                        (varint)
        0x07,                                           // label value index                       (varint)
        0x0b, 0x63, 0xfe, 0x59,                         // time in seconds                         (fixed ui32)
        0x1e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // count                                   (fixed ui64)
        0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x24, 0x40, // sum                                     (fixed double)
        0xcd, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xdc, 0xbf, // min                                     (fixed double)
        0x64, 0x3b, 0xdf, 0x4f, 0x8d, 0x97, 0xde, 0x3f, // max                                     (fixed double)
        0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0xd3, 0x3f, // last                                    (fixed double)
    };

    ui8 expectedMetric8[] = {
        0x26,                                           // types (LOGHIST | ONE_WITH_TS)           (fixed ui8)
        0x00,                                           // flags                                   (fixed ui8)
        0x01,                                           // metric labels count                     (variant)
        0x00,                                           // label name index                        (variant)
        0x08,                                           // label value index                       (variant)
        0x0b, 0x63, 0xfe, 0x59,                         // time in seconds                         (fixed ui32)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x3F, // base                                    (fixed double)
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // zerosCount                              (fixed ui64)
        0x00,                                           // startPower                              (variant)
        0x04,                                           // buckets count                           (variant)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xE0, 0x3F,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xD0, 0x3F, // bucket values
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xD0, 0x3F,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xE0, 0x3F,
    };

    const size_t expectedSize =
        Y_ARRAY_SIZE(expectedHeader) +
        Y_ARRAY_SIZE(expectedStringPools) +
        Y_ARRAY_SIZE(expectedCommonTime) +
        Y_ARRAY_SIZE(expectedCommonLabels) +
        Y_ARRAY_SIZE(expectedMetric1) +
        Y_ARRAY_SIZE(expectedMetric2) +
        Y_ARRAY_SIZE(expectedMetric3) +
        Y_ARRAY_SIZE(expectedMetric4) +
        Y_ARRAY_SIZE(expectedMetric5) +
        Y_ARRAY_SIZE(expectedMetric6) +
        Y_ARRAY_SIZE(expectedMetric7) +
        Y_ARRAY_SIZE(expectedMetric8);

    const TInstant now = TInstant::ParseIso8601Deprecated("2017-11-05T01:02:03Z");

    // {1: 1, 2: 1, 4: 2, 8: 4, 16: 8, inf: 83}
    IHistogramSnapshotPtr TestHistogram() {
        auto h = ExponentialHistogram(6, 2);
        for (i64 i = 1; i < 100; i++) {
            h->Collect(i);
        }
        return h->Snapshot();
    }

    TLogHistogramSnapshotPtr TestLogHistogram() {
        TVector buckets{0.5, 0.25, 0.25, 0.5};
        return MakeIntrusive<TLogHistogramSnapshot>(1.5, 1u, 0, std::move(buckets));
    }

    ISummaryDoubleSnapshotPtr TestSummaryDouble() {
        return MakeIntrusive<TSummaryDoubleSnapshot>(10.1, -0.45, 0.478, 0.3, 30u);
    }

    Y_UNIT_TEST(Encode) {
        TBuffer buffer;
        TBufferOutput out(buffer);
        auto e = EncoderSpackV1(
            &out, ETimePrecision::SECONDS, ECompression::IDENTITY);

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
            e->OnMetricBegin(EMetricType::RATE);
            {
                e->OnLabelsBegin();
                e->OnLabel("name", "q1");
                e->OnLabelsEnd();
            }
            e->OnMetricEnd();
        }
        { // metric #2
            e->OnMetricBegin(EMetricType::COUNTER);
            {
                e->OnLabelsBegin();
                e->OnLabel("name", "q2");
                e->OnLabelsEnd();
            }
            // Only the last value will be encoded
            e->OnUint64(TInstant::Zero(), 10);
            e->OnUint64(TInstant::Zero(), 13);
            e->OnUint64(TInstant::Zero(), 17);
            e->OnMetricEnd();
        }
        { // metric #3
            e->OnMetricBegin(EMetricType::COUNTER);
            {
                e->OnLabelsBegin();
                e->OnLabel("name", "q3");
                e->OnLabelsEnd();
            }
            e->OnUint64(now, 10);
            e->OnUint64(now, 13);
            e->OnUint64(now, 17);
            e->OnMetricEnd();
        }
        { // metric #4
            e->OnMetricBegin(EMetricType::GAUGE);
            {
                e->OnLabelsBegin();
                e->OnLabel("name", "answer");
                e->OnLabelsEnd();
            }
            e->OnDouble(now, 42);
            e->OnDouble(now + TDuration::Seconds(15), 59);
            e->OnMetricEnd();
        }
        { // metric #5
            e->OnMetricBegin(EMetricType::HIST);
            {
                e->OnLabelsBegin();
                e->OnLabel("name", "responseTimeMillis");
                e->OnLabelsEnd();
            }

            auto histogram = TestHistogram();
            e->OnHistogram(now, histogram);
            e->OnMetricEnd();
        }
        { // metric #6
            e->OnMetricBegin(EMetricType::IGAUGE);
            {
                e->OnLabelsBegin();
                e->OnLabel("name", "bytes");
                e->OnLabelsEnd();
            }
            e->OnInt64(now, 1337);
            e->OnMetricEnd();
        }
        { // metric 7
            e->OnMetricBegin(EMetricType::DSUMMARY);
            {
                e->OnLabelsBegin();
                e->OnLabel("name", "temperature");
                e->OnLabelsEnd();
            }
            e->OnSummaryDouble(now, TestSummaryDouble());
            e->OnMetricEnd();
        }
        { // metric 8
            e->OnMetricBegin(EMetricType::LOGHIST);
            {
                e->OnLabelsBegin();
                e->OnLabel("name", "ms");
                e->OnLabelsEnd();
            }
            e->OnLogHistogram(now, TestLogHistogram());
            e->OnMetricEnd();
        }
        e->OnStreamEnd();
        e->Close();

        // Cout << "encoded: " << HexEncode(buffer.Data(), buffer.Size()) << Endl;
        // Cout << "size: " << buffer.Size() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(buffer.Size(), expectedSize);

        ui8* p = reinterpret_cast<ui8*>(buffer.Data());
        UNIT_ASSERT_BINARY_EQUALS(p, expectedHeader);
        p += Y_ARRAY_SIZE(expectedHeader);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedStringPools);
        p += Y_ARRAY_SIZE(expectedStringPools);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedCommonTime);
        p += Y_ARRAY_SIZE(expectedCommonTime);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedCommonLabels);
        p += Y_ARRAY_SIZE(expectedCommonLabels);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedMetric1);
        p += Y_ARRAY_SIZE(expectedMetric1);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedMetric2);
        p += Y_ARRAY_SIZE(expectedMetric2);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedMetric3);
        p += Y_ARRAY_SIZE(expectedMetric3);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedMetric4);
        p += Y_ARRAY_SIZE(expectedMetric4);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedMetric5);
        p += Y_ARRAY_SIZE(expectedMetric5);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedMetric6);
        p += Y_ARRAY_SIZE(expectedMetric6);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedMetric7);
        p += Y_ARRAY_SIZE(expectedMetric7);

        UNIT_ASSERT_BINARY_EQUALS(p, expectedMetric8);
        p += Y_ARRAY_SIZE(expectedMetric8);
    }

    NProto::TMultiSamplesList GetMergingMetricSamples(EMetricsMergingMode mergingMode) {
        TBuffer buffer;
        TBufferOutput out(buffer);

        auto e = EncoderSpackV1(
            &out,
            ETimePrecision::SECONDS,
            ECompression::IDENTITY,
            mergingMode
        );

        e->OnStreamBegin();
        for (size_t i = 0; i != 3; ++i) {
            e->OnMetricBegin(EMetricType::COUNTER);
            {
                e->OnLabelsBegin();
                e->OnLabel("name", "my_counter");
                e->OnLabelsEnd();
            }
            e->OnUint64(TInstant::Zero() + TDuration::Seconds(i), i + 1);
            e->OnMetricEnd();
        }
        e->OnStreamEnd();
        e->Close();

        NProto::TMultiSamplesList samples;
        IMetricEncoderPtr eProto = EncoderProtobuf(&samples);
        TBufferInput in(buffer);
        DecodeSpackV1(&in, eProto.Get());

        return samples;
    }

    Y_UNIT_TEST(SpackEncoderMergesMetrics) {
        {
            NProto::TMultiSamplesList samples = GetMergingMetricSamples(EMetricsMergingMode::DEFAULT);

            UNIT_ASSERT_EQUAL(samples.SamplesSize(), 3);
            UNIT_ASSERT_EQUAL(samples.GetSamples(0).GetPoints(0).GetUint64(), 1);
            UNIT_ASSERT_EQUAL(samples.GetSamples(1).GetPoints(0).GetUint64(), 2);
            UNIT_ASSERT_EQUAL(samples.GetSamples(2).GetPoints(0).GetUint64(), 3);
        }

        {
            NProto::TMultiSamplesList samples = GetMergingMetricSamples(EMetricsMergingMode::MERGE_METRICS);

            UNIT_ASSERT_EQUAL(samples.SamplesSize(), 1);

            auto sample0 = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(sample0.GetPoints(0).GetUint64(), 1);
            UNIT_ASSERT_EQUAL(sample0.GetPoints(1).GetUint64(), 2);
            UNIT_ASSERT_EQUAL(sample0.GetPoints(2).GetUint64(), 3);
        }
    }

    void DecodeDataToSamples(NProto::TMultiSamplesList & samples, ui16 version) {
        IMetricEncoderPtr e = EncoderProtobuf(&samples);

        TBuffer data(expectedSize);
        if (SV1_00 == version) { // v1.0
            data.Append(reinterpret_cast<char*>(expectedHeader_v1_0), Y_ARRAY_SIZE(expectedHeader_v1_0));
        } else {
            data.Append(reinterpret_cast<char*>(expectedHeader), Y_ARRAY_SIZE(expectedHeader));
        }
        data.Append(reinterpret_cast<char*>(expectedStringPools), Y_ARRAY_SIZE(expectedStringPools));
        data.Append(reinterpret_cast<char*>(expectedCommonTime), Y_ARRAY_SIZE(expectedCommonTime));
        data.Append(reinterpret_cast<char*>(expectedCommonLabels), Y_ARRAY_SIZE(expectedCommonLabels));
        data.Append(reinterpret_cast<char*>(expectedMetric1), Y_ARRAY_SIZE(expectedMetric1));
        data.Append(reinterpret_cast<char*>(expectedMetric2), Y_ARRAY_SIZE(expectedMetric2));
        data.Append(reinterpret_cast<char*>(expectedMetric3), Y_ARRAY_SIZE(expectedMetric3));
        data.Append(reinterpret_cast<char*>(expectedMetric4), Y_ARRAY_SIZE(expectedMetric4));
        if (SV1_00 == version) { // v1.0
            data.Append(reinterpret_cast<char*>(expectedMetric5_v1_0), Y_ARRAY_SIZE(expectedMetric5_v1_0));
        } else {
            data.Append(reinterpret_cast<char*>(expectedMetric5), Y_ARRAY_SIZE(expectedMetric5));
        }
        data.Append(reinterpret_cast<char*>(expectedMetric6), Y_ARRAY_SIZE(expectedMetric6));
        data.Append(reinterpret_cast<char*>(expectedMetric7), Y_ARRAY_SIZE(expectedMetric7));
        data.Append(reinterpret_cast<char*>(expectedMetric8), Y_ARRAY_SIZE(expectedMetric8));
        TBufferInput in(data);
        DecodeSpackV1(&in, e.Get());
    }

    void DecodeDataToSamples(NProto::TMultiSamplesList & samples) {
        TSpackHeader header;
        header.Version = SV1_01;
        DecodeDataToSamples(samples, header.Version);
    }

    Y_UNIT_TEST(Decode) {
        NProto::TMultiSamplesList samples;
        DecodeDataToSamples(samples);

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::MilliSeconds(samples.GetCommonTime()),
            TInstant::Seconds(1500000000));

        UNIT_ASSERT_VALUES_EQUAL(samples.CommonLabelsSize(), 1);
        AssertLabelEqual(samples.GetCommonLabels(0), "project", "solomon");

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 8);
        {
            const NProto::TMultiSample& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::RATE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "name", "q1");
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(1);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::COUNTER);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "name", "q2");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), TInstant::Zero(), ui64(17));
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(2);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::COUNTER);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "name", "q3");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), now, ui64(17));
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(3);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "name", "answer");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 2);
            AssertPointEqual(s.GetPoints(0), now, double(42));
            AssertPointEqual(s.GetPoints(1), now + TDuration::Seconds(15), double(59));
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(4);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::HISTOGRAM);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "name", "responseTimeMillis");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);

            const NProto::TPoint& p = s.GetPoints(0);
            UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), now.MilliSeconds());
            UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kHistogram);

            auto histogram = TestHistogram();

            const NProto::THistogram& pointHistogram = p.GetHistogram();
            UNIT_ASSERT_VALUES_EQUAL(pointHistogram.BoundsSize(), histogram->Count());
            UNIT_ASSERT_VALUES_EQUAL(pointHistogram.ValuesSize(), histogram->Count());

            for (size_t i = 0; i < pointHistogram.BoundsSize(); i++) {
                UNIT_ASSERT_DOUBLES_EQUAL(pointHistogram.GetBounds(i), histogram->UpperBound(i), Min<double>());
                UNIT_ASSERT_VALUES_EQUAL(pointHistogram.GetValues(i), histogram->Value(i));
            }
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(5);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::IGAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "name", "bytes");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
            AssertPointEqual(s.GetPoints(0), now, i64(1337));
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(6);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::DSUMMARY);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "name", "temperature");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);

            const NProto::TPoint& p = s.GetPoints(0);
            UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), now.MilliSeconds());
            UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kSummaryDouble);

            auto expected = TestSummaryDouble();

            auto actual = p.GetSummaryDouble();

            UNIT_ASSERT_VALUES_EQUAL(expected->GetSum(), actual.GetSum());
            UNIT_ASSERT_VALUES_EQUAL(expected->GetMin(), actual.GetMin());
            UNIT_ASSERT_VALUES_EQUAL(expected->GetMax(), actual.GetMax());
            UNIT_ASSERT_VALUES_EQUAL(expected->GetLast(), actual.GetLast());
            UNIT_ASSERT_VALUES_EQUAL(expected->GetCount(), actual.GetCount());
        }
        {
            const NProto::TMultiSample& s = samples.GetSamples(7);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::LOGHISTOGRAM);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "name", "ms");

            UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);

            const NProto::TPoint& p = s.GetPoints(0);
            UNIT_ASSERT_VALUES_EQUAL(p.GetTime(), now.MilliSeconds());
            UNIT_ASSERT_EQUAL(p.GetValueCase(), NProto::TPoint::kLogHistogram);

            auto expected = TestLogHistogram();
            auto actual = p.GetLogHistogram();

            UNIT_ASSERT_VALUES_EQUAL(expected->ZerosCount(), actual.GetZerosCount());
            UNIT_ASSERT_VALUES_EQUAL(expected->Base(), actual.GetBase());
            UNIT_ASSERT_VALUES_EQUAL(expected->StartPower(), actual.GetStartPower());
            UNIT_ASSERT_VALUES_EQUAL(expected->Count(), actual.BucketsSize());
            for (size_t i = 0; i < expected->Count(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(expected->Bucket(i), actual.GetBuckets(i));
            }
        }
    }

    void TestCompression(ECompression alg) {
        TBuffer buffer;
        {
            TBufferOutput out(buffer);
            auto e = EncoderSpackV1(&out, ETimePrecision::MILLIS, alg);
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::GAUGE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("name", "answer");
                    e->OnLabelsEnd();
                }
                e->OnDouble(now, 42);
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
            e->Close();
        }

        auto* header = reinterpret_cast<const TSpackHeader*>(buffer.Data());
        UNIT_ASSERT_EQUAL(DecodeCompression(header->Compression), alg);

        NProto::TMultiSamplesList samples;
        {
            IMetricEncoderPtr e = EncoderProtobuf(&samples);
            TBufferInput in(buffer);
            DecodeSpackV1(&in, e.Get());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TInstant::MilliSeconds(samples.GetCommonTime()),
            TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(samples.CommonLabelsSize(), 0);

        UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 1);
        {
            const NProto::TMultiSample& s = samples.GetSamples(0);
            UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::GAUGE);
            UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 1);
            AssertLabelEqual(s.GetLabels(0), "name", "answer");
            AssertPointEqual(s.GetPoints(0), now, 42.0);
        }
    }

    Y_UNIT_TEST(CompressionIdentity) {
        TestCompression(ECompression::IDENTITY);
    }

    Y_UNIT_TEST(CompressionZlib) {
        TestCompression(ECompression::ZLIB);
    }

    Y_UNIT_TEST(CompressionZstd) {
        TestCompression(ECompression::ZSTD);
    }

    Y_UNIT_TEST(CompressionLz4) {
        TestCompression(ECompression::LZ4);
    }

    Y_UNIT_TEST(Decode_v1_0_histograms) {
        // Check that histogram bounds decoded from different versions are the same
        NProto::TMultiSamplesList samples, samples_v1_0;
        DecodeDataToSamples(samples);
        DecodeDataToSamples(samples_v1_0, /*version = */ SV1_00);

        const NProto::THistogram& pointHistogram = samples.GetSamples(4).GetPoints(0).GetHistogram();
        const NProto::THistogram& pointHistogram_v1_0 = samples_v1_0.GetSamples(4).GetPoints(0).GetHistogram();

        for (size_t i = 0; i < pointHistogram.BoundsSize(); i++) {
            UNIT_ASSERT_DOUBLES_EQUAL(pointHistogram.GetBounds(i), pointHistogram_v1_0.GetBounds(i), Min<double>());
        }
    }

    Y_UNIT_TEST(SimpleV12) {
        ui8 expectedSerialized[] = {
            // header
            0x53, 0x50,             // magic "SP"                     (fixed ui16)
        // minor, major
            0x02, 0x01,             // version                        (fixed ui16)
            0x18, 0x00,             // header size                    (fixed ui16)
            0x00,                   // time precision                 (fixed ui8)
            0x00,                   // compression algorithm          (fixed ui8)
            0x0A, 0x00, 0x00, 0x00, // label names size   (fixed ui32)
            0x14, 0x00, 0x00, 0x00, // labels values size (fixed ui32)
            0x01, 0x00, 0x00, 0x00, // metric count       (fixed ui32)
            0x01, 0x00, 0x00, 0x00, // points count       (fixed ui32)

            // string pools
            0x73, 0x00,                                     // "s\0"
            0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x00, // "project\0"
            0x73, 0x6f, 0x6c, 0x6f, 0x6d, 0x6f, 0x6e, 0x00, // "solomon\0"
            0x74, 0x65, 0x6D, 0x70, 0x65, 0x72, 0x61, 0x74, // temperature
            0x75, 0x72, 0x65, 0x00,

            // common time
            0x00, 0x2f, 0x68, 0x59,                         // common time in seconds                  (fixed ui32)

            // common labels
            0x00,                                           // common labels count                     (varint)

            // metric
            0x09,                                           // types (COUNTER | ONE_WITHOUT_TS)        (fixed ui8)
            0x00,                                           // flags                                   (fixed ui8)
            0x01,                                           // name index                              (varint)
            0x01,                                           // metric labels count                     (varint)
            0x01,                                           // 'project' label name index              (varint)
            0x00,                                           // 'project' label value index             (varint)
            0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value (fixed ui64)
        };

        // encode
        {
            TBuffer actualSerialized;
            {
                TBufferOutput out(actualSerialized);
                auto e = EncoderSpackV12(
                    &out,
                    ETimePrecision::SECONDS,
                    ECompression::IDENTITY,
                    EMetricsMergingMode::DEFAULT,
                    "s");

                e->OnStreamBegin();
                e->OnCommonTime(TInstant::Seconds(1500000000));

                {
                    e->OnMetricBegin(EMetricType::COUNTER);
                    {
                        e->OnLabelsBegin();
                        e->OnLabel("project", "solomon");
                        e->OnLabel("s", "temperature");
                        e->OnLabelsEnd();
                    }
                    // Only the last value will be encoded
                    e->OnUint64(TInstant::Zero(), 10);
                    e->OnUint64(TInstant::Zero(), 13);
                    e->OnUint64(TInstant::Zero(), 17);
                    e->OnMetricEnd();
                }

                e->OnStreamEnd();
                e->Close();
            }

            UNIT_ASSERT_VALUES_EQUAL(actualSerialized.Size(), Y_ARRAY_SIZE(expectedSerialized));
            UNIT_ASSERT_BINARY_EQUALS(actualSerialized.Data(), expectedSerialized);
        }

        // decode
        {
            NProto::TMultiSamplesList samples;
            {
                auto input = TMemoryInput(expectedSerialized, Y_ARRAY_SIZE(expectedSerialized));
                auto encoder = EncoderProtobuf(&samples);
                DecodeSpackV1(&input, encoder.Get(), "s");
            }

            UNIT_ASSERT_VALUES_EQUAL(TInstant::MilliSeconds(samples.GetCommonTime()),
                                     TInstant::Seconds(1500000000));

            UNIT_ASSERT_VALUES_EQUAL(samples.CommonLabelsSize(), 0);

            UNIT_ASSERT_VALUES_EQUAL(samples.SamplesSize(), 1);
            {
                const auto& s = samples.GetSamples(0);
                UNIT_ASSERT_EQUAL(s.GetMetricType(), NProto::COUNTER);
                UNIT_ASSERT_VALUES_EQUAL(s.LabelsSize(), 2);
                AssertLabelEqual(s.GetLabels(0), "s", "temperature");
                AssertLabelEqual(s.GetLabels(1), "project", "solomon");

                UNIT_ASSERT_VALUES_EQUAL(s.PointsSize(), 1);
                AssertPointEqual(s.GetPoints(0), TInstant::Zero(), ui64(17));
            }
        }
    }

    Y_UNIT_TEST(V12MissingNameForOneMetric) {
        TBuffer b;
        TBufferOutput out(b);
        auto e = EncoderSpackV12(
            &out,
            ETimePrecision::SECONDS,
            ECompression::IDENTITY,
            EMetricsMergingMode::DEFAULT,
            "s");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            [&]() {
                e->OnStreamBegin();
                {
                    e->OnMetricBegin(EMetricType::COUNTER);
                    {
                        e->OnLabelsBegin();
                        e->OnLabel("s", "s1");
                        e->OnLabelsEnd();
                    }
                    e->OnUint64(TInstant::Zero(), 1);
                    e->OnMetricEnd();

                    e->OnMetricBegin(EMetricType::COUNTER);
                    {
                        e->OnLabelsBegin();
                        e->OnLabel("project", "solomon");
                        e->OnLabel("m", "v");
                        e->OnLabelsEnd();
                    }
                    e->OnUint64(TInstant::Zero(), 2);
                    e->OnMetricEnd();
                }

                e->OnStreamEnd();
                e->Close();
            }(),
            yexception,
            "metric name label 's' not found, all metric labels '{m=v, project=solomon}'");
    }
}

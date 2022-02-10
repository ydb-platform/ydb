#include "text.h"

#include <library/cpp/monlib/metrics/histogram_collector.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TTextText) {
    template <typename TFunc>
    TString EncodeToString(bool humanReadableTs, TFunc fn) {
        TStringStream ss;
        IMetricEncoderPtr encoder = EncoderText(&ss, humanReadableTs);
        fn(encoder.Get());
        return ss.Str();
    }

    Y_UNIT_TEST(Empty) {
        auto result = EncodeToString(true, [](IMetricEncoder* e) {
            e->OnStreamBegin();
            e->OnStreamEnd();
        });
        UNIT_ASSERT_STRINGS_EQUAL(result, "");
    }

    Y_UNIT_TEST(CommonPart) {
        auto result = EncodeToString(true, [](IMetricEncoder* e) {
            e->OnStreamBegin();
            e->OnCommonTime(TInstant::ParseIso8601Deprecated("2017-01-02T03:04:05.006Z"));
            {
                e->OnLabelsBegin();
                e->OnLabel("project", "solomon");
                e->OnLabel("cluster", "man");
                e->OnLabel("service", "stockpile");
                e->OnLabelsEnd();
            }
            e->OnStreamEnd();
        });
        UNIT_ASSERT_STRINGS_EQUAL(result,
                                  "common time: 2017-01-02T03:04:05Z\n"
                                  "common labels: {project='solomon', cluster='man', service='stockpile'}\n");
    }

    Y_UNIT_TEST(Gauges) {
        auto result = EncodeToString(true, [](IMetricEncoder* e) {
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
            e->OnStreamEnd();
        });
        UNIT_ASSERT_STRINGS_EQUAL(result,
                                  "    GAUGE cpuUsage{}\n"
                                  "    GAUGE diskUsage{disk='sda1'} [1000]\n"
                                  "    GAUGE memoryUsage{host='solomon-man-00', dc='man'} [(2017-12-02T12:00:00Z, 1000)]\n"
                                  "    GAUGE bytesRx{host='solomon-sas-01', dc='sas'} [(2017-12-02T12:00:00Z, 2), (2017-12-02T12:00:05Z, 4), (2017-12-02T12:00:10Z, 8)]\n");
    }

    Y_UNIT_TEST(IntGauges) {
        auto result = EncodeToString(true, [](IMetricEncoder* e) {
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
                e->OnDouble(TInstant::Zero(), 1000);
                e->OnMetricEnd();
            }
            { // one value with ts
                e->OnMetricBegin(EMetricType::IGAUGE);
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
                e->OnMetricBegin(EMetricType::IGAUGE);
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
            e->OnStreamEnd();
        });
        UNIT_ASSERT_STRINGS_EQUAL(result,
                                  "   IGAUGE cpuUsage{}\n"
                                  "   IGAUGE diskUsage{disk='sda1'} [1000]\n"
                                  "   IGAUGE memoryUsage{host='solomon-man-00', dc='man'} [(2017-12-02T12:00:00Z, 1000)]\n"
                                  "   IGAUGE bytesRx{host='solomon-sas-01', dc='sas'} [(2017-12-02T12:00:00Z, 2), (2017-12-02T12:00:05Z, 4), (2017-12-02T12:00:10Z, 8)]\n");
    }

    Y_UNIT_TEST(Counters) {
        auto doEncode = [](IMetricEncoder* e) {
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
                e->OnUint64(TInstant::Zero(), 1000);
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
                e->OnUint64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:00Z"), 1000);
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
                e->OnUint64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:00Z"), 2);
                e->OnUint64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:05Z"), 4);
                e->OnUint64(TInstant::ParseIso8601Deprecated("2017-12-02T12:00:10Z"), 8);
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        };

        auto result1 = EncodeToString(false, doEncode);
        UNIT_ASSERT_STRINGS_EQUAL(result1,
                                  "  COUNTER cpuUsage{}\n"
                                  "  COUNTER diskUsage{disk='sda1'} [1000]\n"
                                  "  COUNTER memoryUsage{host='solomon-man-00', dc='man'} [(1512216000, 1000)]\n"
                                  "  COUNTER bytesRx{host='solomon-sas-01', dc='sas'} [(1512216000, 2), (1512216005, 4), (1512216010, 8)]\n");

        auto result2 = EncodeToString(true, doEncode);
        UNIT_ASSERT_STRINGS_EQUAL(result2,
                                  "  COUNTER cpuUsage{}\n"
                                  "  COUNTER diskUsage{disk='sda1'} [1000]\n"
                                  "  COUNTER memoryUsage{host='solomon-man-00', dc='man'} [(2017-12-02T12:00:00Z, 1000)]\n"
                                  "  COUNTER bytesRx{host='solomon-sas-01', dc='sas'} [(2017-12-02T12:00:00Z, 2), (2017-12-02T12:00:05Z, 4), (2017-12-02T12:00:10Z, 8)]\n");
    }

    Y_UNIT_TEST(Histograms) {
        auto h = ExplicitHistogram({1, 2, 3, 4, 5});
        h->Collect(3);
        h->Collect(5, 7);
        h->Collect(13);
        auto s = h->Snapshot();

        TString result = EncodeToString(true, [s](IMetricEncoder* e) {
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::HIST);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "readTimeMillis");
                    e->OnLabelsEnd();
                }
                e->OnHistogram(TInstant::Zero(), s);
                e->OnMetricEnd();
            }
            {
                e->OnMetricBegin(EMetricType::HIST_RATE);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "writeTimeMillis");
                    e->OnLabelsEnd();
                }
                e->OnHistogram(TInstant::Zero(), s);
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        });

        UNIT_ASSERT_STRINGS_EQUAL(result,
                                  "     HIST readTimeMillis{} [{1: 0, 2: 0, 3: 1, 4: 0, 5: 7, inf: 1}]\n"
                                  "HIST_RATE writeTimeMillis{} [{1: 0, 2: 0, 3: 1, 4: 0, 5: 7, inf: 1}]\n");
    }

    Y_UNIT_TEST(Summary) {
        auto s = MakeIntrusive<TSummaryDoubleSnapshot>(10.1, -0.45, 0.478, 0.3, 30u);
        TString result = EncodeToString(true, [s](IMetricEncoder* e) {
            e->OnStreamBegin();
            {
                e->OnMetricBegin(EMetricType::DSUMMARY);
                {
                    e->OnLabelsBegin();
                    e->OnLabel("sensor", "temperature");
                    e->OnLabelsEnd();
                }
                e->OnSummaryDouble(TInstant::Zero(), s);
                e->OnMetricEnd();
            }
            e->OnStreamEnd();
        });
        UNIT_ASSERT_STRINGS_EQUAL(result,
                " DSUMMARY temperature{} [{sum: 10.1, min: -0.45, max: 0.478, last: 0.3, count: 30}]\n");
    }
}

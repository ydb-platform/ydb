#include "json_decoder.cpp"

#include <library/cpp/monlib/consumers/collecting_consumer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>


using namespace NMonitoring;

enum EJsonPart : ui8 {
    METRICS = 0,
    COMMON_TS = 1,
    COMMON_LABELS = 2,
};

constexpr std::array<TStringBuf, 3> JSON_PARTS = {
    TStringBuf(R"("metrics": [{
        "labels": { "key": "value" },
        "type": "GAUGE",
        "value": 123
    }])"),

    TStringBuf(R"("ts": 1)"),

    TStringBuf(R"("commonLabels": {
        "key1": "value1",
        "key2": "value2"
    })"),
};

TString BuildJson(std::initializer_list<EJsonPart> parts) {
    TString data = "{";

    for (auto it = parts.begin(); it != parts.end(); ++it) {
        data += JSON_PARTS[*it];

        if (it + 1 != parts.end()) {
            data += ",";
        }
    }

    data += "}";
    return data;
}

void ValidateCommonParts(TCommonParts&& commonParts, bool checkLabels, bool checkTs) {
    if (checkTs) {
        UNIT_ASSERT_VALUES_EQUAL(commonParts.CommonTime.MilliSeconds(), 1000);
    }

    if (checkLabels) {
        auto& labels = commonParts.CommonLabels;
        UNIT_ASSERT_VALUES_EQUAL(labels.Size(), 2);
        UNIT_ASSERT(labels.Has(TStringBuf("key1")));
        UNIT_ASSERT(labels.Has(TStringBuf("key2")));
        UNIT_ASSERT_VALUES_EQUAL(labels.Get(TStringBuf("key1")).value()->Value(), "value1");
        UNIT_ASSERT_VALUES_EQUAL(labels.Get(TStringBuf("key2")).value()->Value(), "value2");
    }
}

void ValidateMetrics(const TVector<TMetricData>& metrics) {
    UNIT_ASSERT_VALUES_EQUAL(metrics.size(), 1);

    auto& m = metrics[0];
    UNIT_ASSERT_VALUES_EQUAL(m.Kind, EMetricType::GAUGE);
    auto& l = m.Labels;
    UNIT_ASSERT_VALUES_EQUAL(l.Size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(l.Get(0)->Name(), "key");
    UNIT_ASSERT_VALUES_EQUAL(l.Get(0)->Value(), "value");

    UNIT_ASSERT_VALUES_EQUAL(m.Values->Size(), 1);
    UNIT_ASSERT_VALUES_EQUAL((*m.Values)[0].GetValue().AsDouble(), 123);
}

void CheckCommonPartsCollector(TString data, bool shouldBeStopped, bool checkLabels = true, bool checkTs = true, TStringBuf metricNameLabel = "name") {
    TCommonPartsCollector commonPartsCollector;
    TMemoryInput memIn(data);
    TDecoderJson decoder(data, &commonPartsCollector, metricNameLabel);

    bool isOk{false};
    UNIT_ASSERT_NO_EXCEPTION(isOk = NJson::ReadJson(&memIn, &decoder));
    UNIT_ASSERT_VALUES_EQUAL(isOk, !shouldBeStopped);

    ValidateCommonParts(commonPartsCollector.CommonParts(), checkLabels, checkTs);
}

Y_UNIT_TEST_SUITE(TJsonDecoderTest) {
    Y_UNIT_TEST(FullCommonParts) {
        CheckCommonPartsCollector(BuildJson({COMMON_LABELS, COMMON_TS, METRICS}), true);
        CheckCommonPartsCollector(BuildJson({COMMON_TS, COMMON_LABELS, METRICS}), true);

        CheckCommonPartsCollector(BuildJson({METRICS, COMMON_TS, COMMON_LABELS}), true);
        CheckCommonPartsCollector(BuildJson({METRICS, COMMON_LABELS, COMMON_TS}), true);

        CheckCommonPartsCollector(BuildJson({COMMON_LABELS, METRICS, COMMON_TS}), true);
        CheckCommonPartsCollector(BuildJson({COMMON_TS, METRICS, COMMON_LABELS}), true);
    }

    Y_UNIT_TEST(PartialCommonParts) {
        CheckCommonPartsCollector(BuildJson({COMMON_TS, METRICS}), false, false, true);
        CheckCommonPartsCollector(BuildJson({COMMON_LABELS, METRICS}), false, true, false);

        CheckCommonPartsCollector(BuildJson({METRICS, COMMON_LABELS}), false, true, false);
        CheckCommonPartsCollector(BuildJson({METRICS, COMMON_TS}), false, false, true);

        CheckCommonPartsCollector(BuildJson({METRICS}), false, false, false);
    }

    Y_UNIT_TEST(CheckCommonPartsAndMetrics) {
        auto data = BuildJson({COMMON_LABELS, COMMON_TS, METRICS});
        TCollectingConsumer collector;

        DecodeJson(data, &collector);

        TCommonParts commonParts;
        commonParts.CommonTime = collector.CommonTime;
        commonParts.CommonLabels = collector.CommonLabels;

        ValidateCommonParts(std::move(commonParts), true, true);
        ValidateMetrics(collector.Metrics);
    }

    Y_UNIT_TEST(CanParseHistogramsWithInf) {
        const char* metricsData = R"({
"metrics":
    [
        {
            "hist": {
                "bounds": [
                    10
                ],
                "buckets": [
                    11
                ],
                "inf": 12
            },
            "name":"s1",
            "type": "HIST_RATE"
        },
        {
            "hist": {
                "bounds": [
                    20
                ],
                "buckets": [
                    21
                ]
            },
            "name":"s2",
            "type":"HIST_RATE"
        }
    ]
})";
        TCollectingConsumer consumer(false);
        DecodeJson(metricsData, &consumer);

        UNIT_ASSERT_VALUES_EQUAL(consumer.Metrics.size(), 2);
        {
            const auto& m = consumer.Metrics[0];
            UNIT_ASSERT_VALUES_EQUAL(m.Kind, EMetricType::HIST_RATE);
            UNIT_ASSERT_VALUES_EQUAL(m.Values->Size(), 1);
            const auto* histogram = (*m.Values)[0].GetValue().AsHistogram();
            UNIT_ASSERT_VALUES_EQUAL(histogram->Count(), 2);
            UNIT_ASSERT_VALUES_EQUAL(histogram->UpperBound(1), Max<TBucketBound>());
            UNIT_ASSERT_VALUES_EQUAL(histogram->Value(0), 11);
            UNIT_ASSERT_VALUES_EQUAL(histogram->Value(1), 12);
        }
        {
            const auto& m = consumer.Metrics[1];
            UNIT_ASSERT_VALUES_EQUAL(m.Kind, EMetricType::HIST_RATE);
            UNIT_ASSERT_VALUES_EQUAL(m.Values->Size(), 1);
            const auto* histogram = (*m.Values)[0].GetValue().AsHistogram();
            UNIT_ASSERT_VALUES_EQUAL(histogram->Count(), 1);
            UNIT_ASSERT_VALUES_EQUAL(histogram->UpperBound(0), 20);
            UNIT_ASSERT_VALUES_EQUAL(histogram->Value(0), 21);
        }
    }
}

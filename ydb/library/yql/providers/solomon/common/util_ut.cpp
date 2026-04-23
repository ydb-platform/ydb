#include "util.h"

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/map.h>

namespace NYql::NSo {

Y_UNIT_TEST_SUITE(TestSolomonParseSelectors) {
    Y_UNIT_TEST(Basic) {
        TString selectors = "{a = \"a\", b = \"b\"}";
        TSelectors result;

        TSelectors expectedResult = {
            { "b", { "=", "b" } },
            { "a", { "=", "a" } }
        };

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }

    Y_UNIT_TEST(NewFormat) {
        TString selectors = "\"sensor_name\"{a = \"a\", b = \"b\"}";
        TSelectors result;

        TSelectors expectedResult = {
            { "a", { "=", "a" } },
            { "b", { "=", "b" } },
            { "name", { "=", "sensor_name" } }
        };

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }

    Y_UNIT_TEST(Empty) {
        TString selectors = "{}";
        TSelectors result;

        TSelectors expectedResult = {};

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }

    Y_UNIT_TEST(EmptyNewFormat) {
        TString selectors = "\"sensor_name\"{}";
        TSelectors result;

        TSelectors expectedResult = {
            { "name", { "=", "sensor_name" } }
        };

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }

    Y_UNIT_TEST(NoBrackets) {
        TString selectors = "a = \"a\", b = \"b\"";
        TSelectors result;

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format");
    }

    Y_UNIT_TEST(NoQuotes) {
        TString selectors = "{a = a, b = b}";
        TSelectors result;

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format");
    }

    Y_UNIT_TEST(NoQuotesOnSensorName) {
        TString selectors = "sensor_name{a = \"a\", b = \"b\"}";
        TSelectors result;

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format");
    }

    Y_UNIT_TEST(ValidLabelValues) {
        TString selectors = "\"{\"{a = \",\", b = \"}\"}";
        TSelectors result;

        TSelectors expectedResult = {
            { "name", { "=", "{" } },
            { "a", { "=", "," } },
            { "b", { "=", "}" } }
        };

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }

    Y_UNIT_TEST(ComparisonOperators) {
        TString selectors = "\"sensor_name\"{a == \"a\", b != \"b\", c !== \"c\", d =~ \"d\", e !~ \"e\"}";
        TSelectors result;

        TSelectors expectedResult = {
            { "a", { "==", "a" } },
            { "b", { "!=", "b" } },
            { "c", { "!==", "c" } },
            { "d", { "=~", "d" } },
            { "e", { "!~", "e" } },
            { "name", { "=", "sensor_name" } }
        };

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }
}

Y_UNIT_TEST_SUITE(TestSolomonParseLabelNames) {
    Y_UNIT_TEST(Basic) {
        TString labelNames = "label1, label2";
        TVector<TString> names;
        TVector<TString> aliases;

        TVector<TString> expectedNames = {
            "label1", "label2"
        };
        TVector<TString> expectedAliases = {
            "label1", "label2"
        };

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(names, expectedNames);
        UNIT_ASSERT_EQUAL(aliases, expectedAliases);
    }

    Y_UNIT_TEST(WithAliases) {
        TString labelNames = "label1 as alias1, label2 as alias2";
        TVector<TString> names;
        TVector<TString> aliases;

        TVector<TString> expectedNames = {
            "label1", "label2"
        };
        TVector<TString> expectedAliases = {
            "alias1", "alias2"
        };

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(names, expectedNames);
        UNIT_ASSERT_EQUAL(aliases, expectedAliases);
    }

    Y_UNIT_TEST(OneAlias) {
        TString labelNames = "label1, label2 as alias2, label3";
        TVector<TString> names;
        TVector<TString> aliases;

        TVector<TString> expectedNames = {
            "label1", "label2", "label3"
        };
        TVector<TString> expectedAliases = {
            "label1", "alias2", "label3"
        };

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(names, expectedNames);
        UNIT_ASSERT_EQUAL(aliases, expectedAliases);
    }

    Y_UNIT_TEST(CaseSensitivity) {
        TString labelNames = "label1, label2 AS alias2, label3";
        TVector<TString> names;
        TVector<TString> aliases;

        TVector<TString> expectedNames = {
            "label1", "label2", "label3"
        };
        TVector<TString> expectedAliases = {
            "label1", "alias2", "label3"
        };

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(names, expectedNames);
        UNIT_ASSERT_EQUAL(aliases, expectedAliases);
    }

    Y_UNIT_TEST(InvalidLabelName) {
        TString labelNames = "{}, {}";
        TVector<TString> names;
        TVector<TString> aliases;

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format");
    }

    Y_UNIT_TEST(NoAs) {
        TString labelNames = "label1 alias1";
        TVector<TString> names;
        TVector<TString> aliases;

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format");
    }

    Y_UNIT_TEST(EmptyAlias) {
        TString labelNames = "label1 as, label2";
        TVector<TString> names;
        TVector<TString> aliases;

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format");
    }
}

Y_UNIT_TEST_SUITE(TestParseSolomonReadActorConfig) {

    // Helper: build a protobuf map from an initializer list of key-value pairs.
    static google::protobuf::Map<TString, TString> MakeSettings(
        std::initializer_list<std::pair<TString, TString>> pairs)
    {
        google::protobuf::Map<TString, TString> m;
        for (auto& [k, v] : pairs) {
            m[k] = v;
        }
        return m;
    }

    Y_UNIT_TEST(DefaultsWhenEmpty) {
        // An empty settings map must produce the documented default values.
        auto cfg = ParseSolomonReadActorConfig({});

        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxApiInflight,              40u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxListingPageSize,          20000u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.EnablePostApi,               false);
        UNIT_ASSERT_VALUES_EQUAL(cfg.ComputeActorBatchSize,       100u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxDataInflightBytes,        50_MB);
        UNIT_ASSERT_VALUES_EQUAL(cfg.TruePointsFindRangeSec,      301u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxPointsPerOneRequest,      10'000u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.MetricsQueueBatchCountLimit, 100u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.MetricsQueuePrefetchSize,    1000u);
        UNIT_ASSERT_EQUAL(cfg.PoisonTimeout,          TDuration::Hours(3));
        UNIT_ASSERT_EQUAL(cfg.RoundRobinStageTimeout, TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(cfg.LabelsListingLimit,          100'000u);
    }

    Y_UNIT_TEST(OverrideNewFields) {
        // All four new fields can be overridden via the settings map.
        auto cfg = ParseSolomonReadActorConfig(MakeSettings({
            {"maxPointsPerOneRequest", "5000"},
            {"poisonTimeoutSec",       "7200"},
            {"roundRobinStageTimeoutMs", "1500"},
            {"labelsListingLimit",     "50000"},
        }));

        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxPointsPerOneRequest,      5000u);
        UNIT_ASSERT_EQUAL(cfg.PoisonTimeout,          TDuration::Seconds(7200));
        UNIT_ASSERT_EQUAL(cfg.RoundRobinStageTimeout, TDuration::MilliSeconds(1500));
        UNIT_ASSERT_VALUES_EQUAL(cfg.LabelsListingLimit,          50000u);

        // Unrelated fields must keep their defaults.
        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxApiInflight,    40u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.ComputeActorBatchSize, 100u);
    }

    Y_UNIT_TEST(OverrideExistingFields) {
        // Pre-existing fields still parse correctly alongside the new ones.
        auto cfg = ParseSolomonReadActorConfig(MakeSettings({
            {"maxApiInflight",            "8"},
            {"maxListingPageSize",        "1000"},
            {"computeActorBatchSize",     "50"},
            {"maxDataInflightBytes",      "10485760"},  // 10 MB
            {"truePointsFindRange",       "600"},
            {"metricsQueueBatchCountLimit", "200"},
            {"metricsQueuePrefetchSize",  "2000"},
            {"maxPointsPerOneRequest",    "20000"},
            {"poisonTimeoutSec",          "3600"},
            {"roundRobinStageTimeoutMs",  "500"},
            {"labelsListingLimit",        "200000"},
        }));

        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxApiInflight,              8u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxListingPageSize,          1000u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.ComputeActorBatchSize,       50u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxDataInflightBytes,        10485760u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.TruePointsFindRangeSec,      600u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.MetricsQueueBatchCountLimit, 200u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.MetricsQueuePrefetchSize,    2000u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxPointsPerOneRequest,      20000u);
        UNIT_ASSERT_EQUAL(cfg.PoisonTimeout,          TDuration::Seconds(3600));
        UNIT_ASSERT_EQUAL(cfg.RoundRobinStageTimeout, TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(cfg.LabelsListingLimit,          200000u);
    }

    Y_UNIT_TEST(ClampBelowMinimum) {
        // Values below the minimum (1) must be clamped to the minimum.
        auto cfg = ParseSolomonReadActorConfig(MakeSettings({
            {"maxPointsPerOneRequest", "0"},
            {"poisonTimeoutSec",       "0"},
            {"roundRobinStageTimeoutMs", "0"},
            {"labelsListingLimit",     "0"},
        }));

        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxPointsPerOneRequest,      1u);
        UNIT_ASSERT_EQUAL(cfg.PoisonTimeout,          TDuration::Seconds(1));
        UNIT_ASSERT_EQUAL(cfg.RoundRobinStageTimeout, TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(cfg.LabelsListingLimit,          1u);
    }

    Y_UNIT_TEST(InvalidValueFallsBackToDefault) {
        // Non-numeric values must be silently ignored and the default used.
        auto cfg = ParseSolomonReadActorConfig(MakeSettings({
            {"maxPointsPerOneRequest", "not_a_number"},
            {"labelsListingLimit",     "also_bad"},
        }));

        UNIT_ASSERT_VALUES_EQUAL(cfg.MaxPointsPerOneRequest, 10'000u);
        UNIT_ASSERT_VALUES_EQUAL(cfg.LabelsListingLimit,     100'000u);
    }

    Y_UNIT_TEST(RetryConfigParsed) {
        // Retry config fields are parsed correctly.
        auto cfg = ParseSolomonReadActorConfig(MakeSettings({
            {"retryMinDelayMs",          "100"},
            {"retryMinLongRetryDelayMs", "400"},
            {"retryMaxDelayMs",          "2000"},
            {"retryMaxRetries",          "5"},
            {"retryMaxTimeMs",           "60000"},
        }));

        UNIT_ASSERT_EQUAL(cfg.RetryConfig.MinDelay,          TDuration::MilliSeconds(100));
        UNIT_ASSERT_EQUAL(cfg.RetryConfig.MinLongRetryDelay, TDuration::MilliSeconds(400));
        UNIT_ASSERT_EQUAL(cfg.RetryConfig.MaxDelay,          TDuration::MilliSeconds(2000));
        UNIT_ASSERT_VALUES_EQUAL(cfg.RetryConfig.MaxRetries, 5u);
        UNIT_ASSERT_EQUAL(cfg.RetryConfig.MaxTime,           TDuration::MilliSeconds(60000));
    }
}

}  // namespace NYql::NSo

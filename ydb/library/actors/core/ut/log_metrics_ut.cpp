#include <ydb/library/actors/core/log_metrics.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {
    struct TMetricCase {
        TStringBuf Counter;
        TStringBuf Sensor;
        void (ILoggerMetrics::*Increment)();
    };

    const TMetricCase Cases[] = {
        {"ActorMsgs", "logger.actor_msgs", &ILoggerMetrics::IncActorMsgs},
        {"DirectMsgs", "logger.direct_msgs", &ILoggerMetrics::IncDirectMsgs},
        {"LevelRequests", "logger.level_requests", &ILoggerMetrics::IncLevelRequests},
        {"IgnoredMsgs", "logger.ignored_msgs", &ILoggerMetrics::IncIgnoredMsgs},
        {"AlertMsgs", "logger.alert_msgs", &ILoggerMetrics::IncAlertMsgs},
        {"EmergMsgs", "logger.emerg_msgs", &ILoggerMetrics::IncEmergMsgs},
        {"DroppedMsgs", "logger.dropped_msgs", &ILoggerMetrics::IncDroppedMsgs},
    };

    template <typename TRead>
    void CheckIndependentIncrements(ILoggerMetrics& metrics, TRead read) {
        ui64 expected[std::size(Cases)] = {};
        for (size_t i = 0; i < std::size(Cases); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(read(Cases[i]), 0, Cases[i].Counter);
        }
        for (size_t i = 0; i < std::size(Cases); ++i) {
            for (size_t n = 0; n <= i; ++n) {
                (metrics.*Cases[i].Increment)();
                ++expected[i];
                for (size_t j = 0; j < std::size(Cases); ++j) {
                    UNIT_ASSERT_VALUES_EQUAL_C(read(Cases[j]), expected[j], Cases[j].Counter);
                }
            }
        }
    }
}

Y_UNIT_TEST_SUITE(TLoggerMetricsTest) {
    Y_UNIT_TEST(DynamicCountersIncrementOnlyMatchingCounter) {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TLoggerCounters metrics(counters);
        CheckIndependentIncrements(metrics, [&](const TMetricCase& test) {
            auto counter = counters->FindCounter(TString(test.Counter));
            UNIT_ASSERT_C(counter, test.Counter);
            return counter->Val();
        });
        TStringStream html;
        metrics.GetOutputHtml(html);
        UNIT_ASSERT(html.Str().Contains("Counters"));
        for (const auto& test : Cases) {
            UNIT_ASSERT_C(html.Str().Contains(test.Counter), test.Counter);
        }
    }

    Y_UNIT_TEST(RegistryRatesIncrementOnlyMatchingSensor) {
        auto registry = std::make_shared<NMonitoring::TMetricRegistry>();
        TLoggerMetrics metrics(registry);
        CheckIndependentIncrements(metrics, [&](const TMetricCase& test) {
            return registry->Rate({{"sensor", TString(test.Sensor)}})->Get();
        });
        TStringStream html;
        metrics.GetOutputHtml(html);
        UNIT_ASSERT(html.Str().Contains("Metrics"));
    }

    Y_UNIT_TEST(MetricsKeepRegistryAliveForCounterLifetime) {
        auto registry = std::make_shared<NMonitoring::TMetricRegistry>();
        std::weak_ptr<NMonitoring::TMetricRegistry> weak = registry;
        {
            TLoggerMetrics metrics(registry);
            registry.reset();
            UNIT_ASSERT(!weak.expired());
            metrics.IncDroppedMsgs();
            UNIT_ASSERT_VALUES_EQUAL(weak.lock()->Rate({{"sensor", "logger.dropped_msgs"}})->Get(), 1);
        }
        UNIT_ASSERT(weak.expired());
    }
}

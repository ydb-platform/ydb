#include "actor_benchmark_helper.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/generic/vector.h>
#include <cstdlib>

using namespace NActors;
using namespace NActors::NTests;


struct THeavyActorBenchmarkSettings : TActorBenchmarkSettings {
    static constexpr ui32 TotalEventsAmountPerThread = 1'000'000;

    static constexpr auto MailboxTypes = {
        TMailboxType::HTSwap,
    };
};


Y_UNIT_TEST_SUITE(HeavyActorBenchmark) {

    using TActorBenchmark = ::NActors::NTests::TActorBenchmark<THeavyActorBenchmarkSettings>;
    using TSettings = TActorBenchmark::TSettings;

    Y_UNIT_TEST(SendActivateReceiveCSVManual) {
        if (const char* testMode = getenv("ACTORSYSTEM_TEST_MODE"); !testMode || TString(testMode) != "manual") {
            return;
        }

        std::vector<ui32> threadsList;
        std::vector<ui32> actorPairsList;
        std::vector<ui32> inflights;
        TDuration duration = TDuration::Seconds(1);

        auto parseUintList = [](const char* envVar) -> std::vector<ui32> {
            std::vector<ui32> result;
            if (const char* envValue = getenv(envVar)) {
                TVector<TString> parts;
                Split(TString(envValue), ",", parts);
                for (const auto& part : parts) {
                    result.push_back(FromString<ui32>(part));
                }
            }
            return result;
        };

        threadsList = parseUintList("ACTORSYSTEM_THREADS");
        actorPairsList = parseUintList("ACTORSYSTEM_ACTOR_PAIRS");
        inflights = parseUintList("ACTORSYSTEM_INFLIGHTS");

        if (const char* durationStr = getenv("ACTORSYSTEM_DURATION")) {
            duration = TDuration::Seconds(FromString<ui64>(durationStr));
        }

        TActorBenchmark::RunSendActivateReceiveCSV(threadsList, actorPairsList, inflights, duration);
    }

    Y_UNIT_TEST(StarSendActivateReceiveCSVManual) {
        if (const char* testMode = getenv("ACTORSYSTEM_TEST_MODE"); !testMode || TString(testMode) != "manual") {
            return;
        }

        std::vector<ui32> threadsList;
        std::vector<ui32> actorPairsList;
        std::vector<ui32> starsList;

        auto parseUintList = [](const char* envVar) -> std::vector<ui32> {
            std::vector<ui32> result;
            if (const char* envValue = getenv(envVar)) {
                TVector<TString> parts;
                Split(TString(envValue), ",", parts);
                for (const auto& part : parts) {
                    result.push_back(FromString<ui32>(part));
                }
            }
            return result;
        };

        threadsList = parseUintList("ACTORSYSTEM_THREADS");
        actorPairsList = parseUintList("ACTORSYSTEM_ACTOR_PAIRS");
        starsList = parseUintList("ACTORSYSTEM_STARS");

        TActorBenchmark::RunStarSendActivateReceiveCSV(threadsList, actorPairsList, starsList);
    }

    Y_UNIT_TEST(SendActivateReceiveCSV) {
        std::vector<ui32> threadsList;
        for (ui32 threads = 1; threads <= 8; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {512};
        TActorBenchmark::RunSendActivateReceiveCSV(threadsList, actorPairsList, {1}, TDuration::Seconds(1));
    }

}

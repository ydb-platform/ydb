#include "actor_benchmark_helper.h"

#include <library/cpp/testing/unittest/registar.h>


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


    Y_UNIT_TEST(SendActivateReceiveCSV) {
        std::vector<ui32> threadsList;
        for (ui32 threads = 1; threads <= 64; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {512};
        TActorBenchmark::RunSendActivateReceiveCSV(threadsList, actorPairsList, {1}, TDuration::Seconds(1));
    }

    Y_UNIT_TEST(SendActivateReceiveCSV_1Pair) {
        std::vector<ui32> threadsList;
        for (ui32 threads = 1; threads <= 64; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {1};
        TActorBenchmark::RunSendActivateReceiveCSV(threadsList, actorPairsList, {1}, TDuration::Seconds(1));
    }

    Y_UNIT_TEST(SendActivateReceiveCSV_10Pairs) {
        std::vector<ui32> threadsList;
        for (ui32 threads = 1; threads <= 64; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {10};
        TActorBenchmark::RunSendActivateReceiveCSV(threadsList, actorPairsList, {1}, TDuration::Seconds(1));
    }


    Y_UNIT_TEST(SendActivateReceiveCSV_10Pairs_LONG) {
        std::vector<ui32> threadsList;
        threadsList.push_back(11);
        std::vector<ui32> actorPairsList = {10};
        TActorBenchmark::RunSendActivateReceiveCSV(threadsList, actorPairsList, {1}, TDuration::Seconds(3600));
    }

    Y_UNIT_TEST(SendActivateReceiveCSV_100Pairs) {
        std::vector<ui32> threadsList;
        for (ui32 threads = 1; threads <= 64; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {100};
        TActorBenchmark::RunSendActivateReceiveCSV(threadsList, actorPairsList, {1}, TDuration::Seconds(1));
    }

    Y_UNIT_TEST(StarSendActivateReceiveCSV_1Pair) {
        std::vector<ui32> threadsList;
        for (ui32 threads = 1; threads <= 64; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {1};
        std::vector<ui32> starsList = {10};
        TActorBenchmark::RunStarSendActivateReceiveCSV(threadsList, actorPairsList, starsList);
    }

    Y_UNIT_TEST(StarSendActivateReceiveCSV_10Pairs) {
        std::vector<ui32> threadsList;
        for (ui32 threads = 1; threads <= 64; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {10};
        std::vector<ui32> starsList = {10};
        TActorBenchmark::RunStarSendActivateReceiveCSV(threadsList, actorPairsList, starsList);
    }

    Y_UNIT_TEST(StarSendActivateReceiveCSV_100Pairs) {
        std::vector<ui32> threadsList;
        for (ui32 threads = 1; threads <= 64; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {100};
        std::vector<ui32> starsList = {10};
        TActorBenchmark::RunStarSendActivateReceiveCSV(threadsList, actorPairsList, starsList);
    }

}

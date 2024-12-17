

#include <ydb/library/actors/core/actor_benchmark_helper.h>

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
        for (ui32 threads = 1; threads <= 28; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {512};
        TActorBenchmark::RunSendActivateReceiveCSV(threadsList, actorPairsList, {1}, TDuration::Seconds(1));
    }

    Y_UNIT_TEST(StarSendActivateReceiveCSV) {
        std::vector<ui32> threadsList;
        for (ui32 threads = 1; threads <= 28; threads++) {
            threadsList.push_back(threads);
        }
        std::vector<ui32> actorPairsList = {512};
        std::vector<ui32> starsList = {10};
        TActorBenchmark::RunStarSendActivateReceiveCSV(threadsList, actorPairsList, starsList);
    }

}

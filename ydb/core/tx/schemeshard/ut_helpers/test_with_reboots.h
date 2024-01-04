#pragma once

#include "test_env.h"

namespace NSchemeShardUT_Private {

class TTestWithTabletReboots: public TTestWithReboots {
public:
    void Run(std::function<void(TTestActorRuntime& runtime, bool& activeZone)> testScenario) {
        TDatashardLogBatchingSwitch logBatchingSwitch(false /* without batching */);
        RunWithTabletReboots(testScenario);
    }
};

class TTestWithPipeResets: public TTestWithReboots {
public:
    void Run(std::function<void(TTestActorRuntime& runtime, bool& activeZone)> testScenario) {
        TDatashardLogBatchingSwitch logBatchingSwitch(false /* without batching */);
        RunWithPipeResets(testScenario);
    }
};

#define Y_UNIT_TEST_WITH_REBOOTS(N)                         \
    template <typename T> void N(NUnitTest::TTestContext&); \
    struct TTestRegistration##N {                           \
        TTestRegistration##N() {                            \
            TCurrentTest::AddTest(#N "[TabletReboots]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<TTestWithTabletReboots>), false); \
            TCurrentTest::AddTest(#N "[PipeResets]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<TTestWithPipeResets>), false); \
        }                                                   \
    };                                                      \
    static TTestRegistration##N testRegistration##N;        \
    template <typename T>                                   \
    void N(NUnitTest::TTestContext&)

}

#pragma once

#include "test_env.h"

namespace NSchemeShardUT_Private {

class TTestWithTabletReboots: public TTestWithReboots {
public:
    explicit TTestWithTabletReboots(bool killOnCommit = false)
        : TTestWithReboots(killOnCommit)
    {}
    void Run(std::function<void(TTestActorRuntime& runtime, bool& activeZone)> testScenario) {
        TDatashardLogBatchingSwitch logBatchingSwitch(false /* without batching */);
        RunWithTabletReboots(testScenario);
    }
};

class TTestWithPipeResets: public TTestWithReboots {
public:
    explicit TTestWithPipeResets(bool killOnCommit = false)
        : TTestWithReboots(killOnCommit)
    {}
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

#define Y_UNIT_TEST_WITH_REBOOTS_FLAG(N, OPT)               \
    template <typename T, bool OPT>                         \
    void N(NUnitTest::TTestContext&);                       \
    struct TTestRegistration##N {                           \
        TTestRegistration##N() {                            \
            TCurrentTest::AddTest(#N "-" #OPT "[TabletReboots]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<TTestWithTabletReboots, false>), false); \
            TCurrentTest::AddTest(#N "-" #OPT "[PipeResets]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<TTestWithPipeResets, false>), false); \
            TCurrentTest::AddTest(#N "+" #OPT "[TabletReboots]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<TTestWithTabletReboots, true>), false); \
            TCurrentTest::AddTest(#N "+" #OPT "[PipeResets]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<TTestWithPipeResets, true>), false); \
        }                                                   \
    };                                                      \
    static TTestRegistration##N testRegistration##N;        \
    template <typename T, bool OPT>                         \
    void N(NUnitTest::TTestContext&)

}

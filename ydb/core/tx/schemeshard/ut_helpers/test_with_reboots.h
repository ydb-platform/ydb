#pragma once

#include "test_env.h"

namespace NSchemeShardUT_Private {

class TTestWithTabletReboots: public TTestWithReboots {
public:
    explicit TTestWithTabletReboots(bool killOnCommit = false)
        : TTestWithReboots(killOnCommit)
    {}
    void Run(std::function<void(TTestActorRuntime& runtime, bool& activeZone)> testScenario) override {
        TDatashardLogBatchingSwitch logBatchingSwitch(false /* without batching */);
        RunWithTabletReboots(testScenario);
    }
};

class TTestWithPipeResets: public TTestWithReboots {
public:
    explicit TTestWithPipeResets(bool killOnCommit = false)
        : TTestWithReboots(killOnCommit)
    {}
    void Run(std::function<void(TTestActorRuntime& runtime, bool& activeZone)> testScenario) override {
        TDatashardLogBatchingSwitch logBatchingSwitch(false /* without batching */);
        RunWithPipeResets(testScenario);
    }
};

#define Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(N, REBOOT_BUCKETS, PIPE_RESET_BUCKETS, KILL_ON_COMMIT) \
    void N(TTestWithReboots& t);                                            \
    struct TTestRegistration##N {                                           \
        std::vector<std::string> names;                                     \
        TTestRegistration##N() {                                            \
            names.reserve(REBOOT_BUCKETS + PIPE_RESET_BUCKETS);             \
            for (int i = 0; i < REBOOT_BUCKETS; i++) {                      \
                std::string name = (REBOOT_BUCKETS > 1                      \
                    ? (#N "[TabletRebootsBucket") + std::to_string(i) + "]" \
                    : (#N "[TabletReboots]"));                              \
                names.push_back(name);                                      \
                TCurrentTest::AddTest(names.back().c_str(),                 \
                    [i](NUnitTest::TTestContext&) {                         \
                        TTestWithTabletReboots t(KILL_ON_COMMIT);           \
                        t.TotalBuckets = REBOOT_BUCKETS;                    \
                        t.Bucket = i;                                       \
                        N(t);                                               \
                    }, false);                                              \
            }                                                               \
            for (int i = 0; i < PIPE_RESET_BUCKETS; i++) {                  \
                std::string name = (PIPE_RESET_BUCKETS > 1                  \
                    ? (#N "[PipeResetsBucket") + std::to_string(i) + "]"    \
                    : (#N "[PipeResets]"));                                 \
                names.push_back(name);                                      \
                TCurrentTest::AddTest(names.back().c_str(),                 \
                    [i](NUnitTest::TTestContext&) {                         \
                        TTestWithPipeResets t(KILL_ON_COMMIT);              \
                        t.TotalBuckets = PIPE_RESET_BUCKETS;                \
                        t.Bucket = i;                                       \
                        N(t);                                               \
                    }, false);                                              \
            }                                                               \
        }                                                                   \
    };                                                                      \
    static TTestRegistration##N testRegistration##N;                        \
    void N(TTestWithReboots& t)
}

#define Y_UNIT_TEST_WITH_REBOOTS(N) Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(N, 1, 1, false)

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/messagebus/rain_check/test/helper/misc.h>
#include <library/cpp/messagebus/rain_check/test/ut/test.h>

#include <library/cpp/messagebus/latch.h>

#include <util/system/event.h>

#include <array>

using namespace NRainCheck;
using namespace NActor;

Y_UNIT_TEST_SUITE(Spawn) {
    struct TTestTask: public ISimpleTask {
        TTestSync* const TestSync;

        TTestTask(TSimpleEnv*, TTestSync* testSync)
            : TestSync(testSync)
            , I(0)
        {
        }

        TSystemEvent Started;

        unsigned I;

        TContinueFunc Start() override {
            if (I < 4) {
                I += 1;
                return &TTestTask::Start;
            }
            TestSync->CheckAndIncrement(0);
            return &TTestTask::Continue;
        }

        TContinueFunc Continue() {
            TestSync->CheckAndIncrement(1);

            Started.Signal();
            return nullptr;
        }
    };

    Y_UNIT_TEST(Continuation) {
        TTestSync testSync;

        TSimpleEnv env;

        TIntrusivePtr<TSimpleTaskRunner> task = env.SpawnTask<TTestTask>(&testSync);

        testSync.WaitForAndIncrement(2);
    }

    struct TSubtask: public ISimpleTask {
        TTestEnv* const Env;
        TTestSync* const TestSync;

        TSubtask(TTestEnv* env, TTestSync* testSync)
            : Env(env)
            , TestSync(testSync)
        {
        }

        TContinueFunc Start() override {
            Sleep(TDuration::MilliSeconds(1));
            TestSync->CheckAndIncrement(1);
            return nullptr;
        }
    };

    struct TSpawnTask: public ISimpleTask {
        TTestEnv* const Env;
        TTestSync* const TestSync;

        TSpawnTask(TTestEnv* env, TTestSync* testSync)
            : Env(env)
            , TestSync(testSync)
        {
        }

        TSubtaskCompletion SubtaskCompletion;

        TContinueFunc Start() override {
            TestSync->CheckAndIncrement(0);
            SpawnSubtask<TSubtask>(Env, &SubtaskCompletion, TestSync);
            return &TSpawnTask::Continue;
        }

        TContinueFunc Continue() {
            TestSync->CheckAndIncrement(2);
            return nullptr;
        }
    };

    Y_UNIT_TEST(Subtask) {
        TTestSync testSync;

        TTestEnv env;

        TIntrusivePtr<TSimpleTaskRunner> task = env.SpawnTask<TSpawnTask>(&testSync);

        testSync.WaitForAndIncrement(3);
    }

    struct TSpawnLongTask: public ISimpleTask {
        TTestEnv* const Env;
        TTestSync* const TestSync;
        unsigned I;

        TSpawnLongTask(TTestEnv* env, TTestSync* testSync)
            : Env(env)
            , TestSync(testSync)
            , I(0)
        {
        }

        std::array<TSubtaskCompletion, 3> Subtasks;

        TContinueFunc Start() override {
            if (I == 1000) {
                TestSync->CheckAndIncrement(0);
                return nullptr;
            }

            for (auto& subtask : Subtasks) {
                SpawnSubtask<TNopSimpleTask>(Env, &subtask, "");
            }

            ++I;
            return &TSpawnLongTask::Start;
        }
    };

    Y_UNIT_TEST(SubtaskLong) {
        TTestSync testSync;

        TTestEnv env;

        TIntrusivePtr<TSimpleTaskRunner> task = env.SpawnTask<TSpawnLongTask>(&testSync);

        testSync.WaitForAndIncrement(1);
    }
}

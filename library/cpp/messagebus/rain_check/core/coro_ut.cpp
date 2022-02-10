#include <library/cpp/testing/unittest/registar.h>

#include "coro.h"
#include "spawn.h"

#include <library/cpp/messagebus/rain_check/test/ut/test.h>

using namespace NRainCheck;

Y_UNIT_TEST_SUITE(RainCheckCoro) {
    struct TSimpleCoroTask : ICoroTask {
        TTestSync* const TestSync;

        TSimpleCoroTask(TTestEnv*, TTestSync* testSync)
            : TestSync(testSync)
        {
        }

        void Run() override {
            TestSync->WaitForAndIncrement(0);
        }
    };

    Y_UNIT_TEST(Simple) {
        TTestSync testSync;

        TTestEnv env;

        TIntrusivePtr<TCoroTaskRunner> task = env.SpawnTask<TSimpleCoroTask>(&testSync);
        testSync.WaitForAndIncrement(1);
    }

    struct TSleepCoroTask : ICoroTask {
        TTestEnv* const Env;
        TTestSync* const TestSync;

        TSleepCoroTask(TTestEnv* env, TTestSync* testSync)
            : Env(env)
            , TestSync(testSync)
        {
        }

        TSubtaskCompletion SleepCompletion;

        void Run() override {
            Env->SleepService.Sleep(&SleepCompletion, TDuration::MilliSeconds(1));
            WaitForSubtasks();
            TestSync->WaitForAndIncrement(0);
        }
    };

    Y_UNIT_TEST(Sleep) {
        TTestSync testSync;

        TTestEnv env;

        TIntrusivePtr<TCoroTaskRunner> task = env.SpawnTask<TSleepCoroTask>(&testSync);

        testSync.WaitForAndIncrement(1);
    }

    struct TSubtask : ICoroTask {
        TTestEnv* const Env;
        TTestSync* const TestSync;

        TSubtask(TTestEnv* env, TTestSync* testSync)
            : Env(env)
            , TestSync(testSync)
        {
        }

        void Run() override {
            TestSync->CheckAndIncrement(1);
        }
    };

    struct TSpawnCoroTask : ICoroTask {
        TTestEnv* const Env;
        TTestSync* const TestSync;

        TSpawnCoroTask(TTestEnv* env, TTestSync* testSync)
            : Env(env)
            , TestSync(testSync)
        {
        }

        TSubtaskCompletion SubtaskCompletion;

        void Run() override {
            TestSync->CheckAndIncrement(0);
            SpawnSubtask<TSubtask>(Env, &SubtaskCompletion, TestSync);
            WaitForSubtasks();
            TestSync->CheckAndIncrement(2);
        }
    };

    Y_UNIT_TEST(Spawn) {
        TTestSync testSync;

        TTestEnv env;

        TIntrusivePtr<TCoroTaskRunner> task = env.SpawnTask<TSpawnCoroTask>(&testSync);

        testSync.WaitForAndIncrement(3);
    }
}

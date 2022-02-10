#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/messagebus/rain_check/test/ut/test.h>

#include <util/system/event.h>

using namespace NRainCheck;
using namespace NActor;

Y_UNIT_TEST_SUITE(Sleep) {
    struct TTestTask: public ISimpleTask {
        TSimpleEnv* const Env;
        TTestSync* const TestSync;

        TTestTask(TSimpleEnv* env, TTestSync* testSync)
            : Env(env)
            , TestSync(testSync)
        {
        }

        TSubtaskCompletion Sleep;

        TContinueFunc Start() override {
            Env->SleepService.Sleep(&Sleep, TDuration::MilliSeconds(1));

            TestSync->CheckAndIncrement(0);

            return &TTestTask::Continue;
        }

        TContinueFunc Continue() {
            TestSync->CheckAndIncrement(1);
            return nullptr;
        }
    };

    Y_UNIT_TEST(Test) {
        TTestSync testSync;

        TSimpleEnv env;

        TIntrusivePtr<TSimpleTaskRunner> task = env.SpawnTask<TTestTask>(&testSync);

        testSync.WaitForAndIncrement(2);
    }
}

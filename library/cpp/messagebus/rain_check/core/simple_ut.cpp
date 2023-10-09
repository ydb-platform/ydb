#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/messagebus/rain_check/test/ut/test.h>

#include <library/cpp/messagebus/latch.h>

#include <util/system/event.h>

using namespace NRainCheck;

Y_UNIT_TEST_SUITE(RainCheckSimple) {
    struct TTaskWithCompletionCallback: public ISimpleTask {
        TTestEnv* const Env;
        TTestSync* const TestSync;

        TTaskWithCompletionCallback(TTestEnv* env, TTestSync* testSync)
            : Env(env)
            , TestSync(testSync)
        {
        }

        TSubtaskCompletion SleepCompletion;

        TContinueFunc Start() override {
            TestSync->CheckAndIncrement(0);

            Env->SleepService.Sleep(&SleepCompletion, TDuration::MilliSeconds(1));
            SleepCompletion.SetCompletionCallback(&TTaskWithCompletionCallback::SleepCompletionCallback);

            return &TTaskWithCompletionCallback::Last;
        }

        void SleepCompletionCallback(TSubtaskCompletion* completion) {
            Y_ABORT_UNLESS(completion == &SleepCompletion);
            TestSync->CheckAndIncrement(1);

            Env->SleepService.Sleep(&SleepCompletion, TDuration::MilliSeconds(1));
            SleepCompletion.SetCompletionCallback(&TTaskWithCompletionCallback::NextSleepCompletionCallback);
        }

        void NextSleepCompletionCallback(TSubtaskCompletion*) {
            TestSync->CheckAndIncrement(2);
        }

        TContinueFunc Last() {
            TestSync->CheckAndIncrement(3);
            return nullptr;
        }
    };

    Y_UNIT_TEST(CompletionCallback) {
        TTestEnv env;
        TTestSync testSync;

        env.SpawnTask<TTaskWithCompletionCallback>(&testSync);

        testSync.WaitForAndIncrement(4);
    }
}

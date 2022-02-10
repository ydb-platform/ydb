#include <library/cpp/testing/unittest/registar.h>

#include "track.h"

#include <library/cpp/messagebus/rain_check/test/helper/misc.h>
#include <library/cpp/messagebus/rain_check/test/ut/test.h>

using namespace NRainCheck;

Y_UNIT_TEST_SUITE(TaskTracker) {
    struct TTaskForTracker: public ISimpleTask {
        TTestSync* const TestSync;

        TTaskForTracker(TTestEnv*, TTestSync* testSync)
            : TestSync(testSync)
        {
        }

        TContinueFunc Start() override {
            TestSync->WaitForAndIncrement(0);
            TestSync->WaitForAndIncrement(2);
            return nullptr;
        }
    };

    Y_UNIT_TEST(Simple) {
        TTestEnv env;

        TIntrusivePtr<TTaskTracker> tracker(new TTaskTracker(env.GetExecutor()));

        TTestSync testSync;

        tracker->Spawn<TTaskForTracker>(&env, &testSync);

        testSync.WaitFor(1);

        UNIT_ASSERT_VALUES_EQUAL(1u, tracker->Size());

        testSync.CheckAndIncrement(1);

        testSync.WaitForAndIncrement(3);

        tracker->Shutdown();
    }
}

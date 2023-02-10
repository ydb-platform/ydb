#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(BigCluster) {

    Y_UNIT_TEST(Test) {
        for (ui32 num : {10, NSan::MSanIsOn() ? 20 : 100}) {
            THPTimer timer;
            {
                TEnvironmentSetup env(num, nullptr);
                env.CreateBoxAndPool(4, 4 * 8 * num);
                env.Cleanup();
                Cerr << "num# " << num << " restart" << Endl;
                env.Initialize();
                env.Sim(TDuration::Minutes(5));
            }
            Cerr << "num# " << num << " passed# " << TDuration::Seconds(timer.Passed()) << Endl;
        }
    }

}

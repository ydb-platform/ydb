#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TRtmrTest) {
    Y_UNIT_TEST(CreateWithoutTimeCastBuckets) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateRtmrVolume(runtime, txId++,  "/MyRoot",
                            "Name: \"rtmr1\" "
                            "PartitionsCount: 0",
                            {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, 100);

        TestLs(runtime, "/MyRoot/rtmr1", false, NLs::PathExist);
    }
}

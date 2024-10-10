#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TBackupCollectionWithRebootsTests) {
    void SetupLogging(TTestActorRuntimeBase& runtime) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    }

    Y_UNIT_TEST(Create) {
        // TODO
        // TTestWithReboots t(false);
        // t.GetTestEnvOptions().InitYdbDriver(true);

        // t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
        //     {
        //         TInactiveZone inactive(activeZone);
        //         SetupLogging(runtime);
        //     }

        //     TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot", DefaultScheme("BackupCollection"));
        //     t.TestEnv->TestWaitNotification(runtime, t.TxId);

        //     TestLs(runtime, "/MyRoot/BackupCollection", false, NLs::PathExist);
        // });
    }

} // TBackupCollectionWithRebootsTests

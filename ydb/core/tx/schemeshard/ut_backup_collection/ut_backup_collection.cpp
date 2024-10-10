#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TBackupCollectionTests) {
    void SetupLogging(TTestActorRuntimeBase& runtime) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    }

    Y_UNIT_TEST(Create) {
        // TODO
        // TTestBasicRuntime runtime;
        // TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        // ui64 txId = 100;

        // SetupLogging(runtime);

        // TestCreateBackupCollection(runtime, ++txId, "/MyRoot", DefaultScheme("BackupCollection"));
        // env.TestWaitNotification(runtime, txId);

        // TestDescribeResult(DescribePath(runtime, "/MyRoot/BackupCollection"), {
        //     NLs::PathExist,
        //     NLs::BackupCollectionState(NKikimrBackupCollection::TBackupCollectionState::kStandBy),
        // });
    }

} // TBackupCollectionTests

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

namespace {
    TString CreateTestShardConfig(const TString& name, ui64 count = 1) {
        return TStringBuilder() << R"(
                Name: ")" << name << R"("
                Count: )" << count << R"(
                StorageConfig {
                }
                CmdInitialize {
                    MaxDataBytes: 1000
                }
            )";
    }
}

Y_UNIT_TEST_SUITE(TTestShardTest) {
    Y_UNIT_TEST(CreateTestShard) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("MyTestShard"), {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyTestShard", false, NLs::PathExist);
    }

    Y_UNIT_TEST(DropTestShard) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("MyTestShard"), {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyTestShard", false, NLs::PathExist);

        TestDropTestShard(runtime, ++txId, "/MyRoot", "MyTestShard");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyTestShard", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(DropTestShardTwice) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("MyTestShard"), {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        AsyncDropTestShard(runtime, ++txId, "/MyRoot", "MyTestShard");
        AsyncDropTestShard(runtime, ++txId, "/MyRoot", "MyTestShard");
        TestModificationResult(runtime, txId - 1);

        const auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>();
        UNIT_ASSERT(ev);

        const auto& record = ev->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusMultipleModifications);
        UNIT_ASSERT_VALUES_EQUAL(record.GetPathDropTxId(), txId - 1);

        env.TestWaitNotification(runtime, txId - 1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/MyTestShard"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(DropTestShardFailOnNotExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestDropTestShard(runtime, ++txId, "/MyRoot", "MyTestShard", {{NKikimrScheme::StatusPathDoesNotExist, "error: path hasn't been resolved"}});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateExistingTestShard) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("MyTestShard"), {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/MyTestShard", false, NLs::PathExist);

        TestCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("MyTestShard"), {NKikimrScheme::StatusAlreadyExists});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AsyncCreateDifferentTestShards) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        AsyncCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("TestShard1"));
        AsyncCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("TestShard2"));

        TestModificationResult(runtime, txId - 1);
        TestModificationResult(runtime, txId);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestLs(runtime, "/MyRoot/TestShard1", false, NLs::PathExist);
        TestLs(runtime, "/MyRoot/TestShard2", false, NLs::PathExist);
    }

    Y_UNIT_TEST(AsyncCreateSameTestShard) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        AsyncCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("MyTestShard"));
        AsyncCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("MyTestShard"));

        TestModificationResults(runtime, txId - 1, {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusAlreadyExists});
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusAlreadyExists});
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestLs(runtime, "/MyRoot/MyTestShard", false, NLs::PathExist);
    }

    Y_UNIT_TEST(ReadOnlyMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        SetSchemeshardReadOnlyMode(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("MyTestShard"), {NKikimrScheme::StatusReadOnly});
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/MyTestShard", false, NLs::PathNotExist);

        SetSchemeshardReadOnlyMode(runtime, false);
        sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateTestShard(runtime, ++txId, "/MyRoot", CreateTestShardConfig("MyTestShard"), {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/MyTestShard", false, NLs::PathExist);
    }

    Y_UNIT_TEST(EmptyName) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShard(runtime, ++txId, "/MyRoot", R"(
                Name: ""
                Count: 1
                StorageConfig {
                }
                CmdInitialize {
                    MaxDataBytes: 1000
                }
            )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);
    }
}

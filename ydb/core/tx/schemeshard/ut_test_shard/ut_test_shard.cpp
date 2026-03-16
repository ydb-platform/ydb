#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

namespace {
    TString CreateTestShardSetConfig(const TString& name, ui64 count = 1) {
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
    Y_UNIT_TEST(CreateTestShardSet) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet"), {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyTestShardSet", false, NLs::PathExist);
    }

    Y_UNIT_TEST(DropTestShardSet) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet"), {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyTestShardSet", false, NLs::PathExist);

        TestDropTestShardSet(runtime, ++txId, "/MyRoot", "MyTestShardSet");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyTestShardSet", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(DropTestShardSetTwice) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet"), {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        AsyncDropTestShardSet(runtime, ++txId, "/MyRoot", "MyTestShardSet");
        AsyncDropTestShardSet(runtime, ++txId, "/MyRoot", "MyTestShardSet");
        TestModificationResult(runtime, txId - 1);

        const auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>();
        UNIT_ASSERT(ev);

        const auto& record = ev->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusMultipleModifications);
        UNIT_ASSERT_VALUES_EQUAL(record.GetPathDropTxId(), txId - 1);

        env.TestWaitNotification(runtime, txId - 1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/MyTestShardSet"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(DropTestShardSetFailOnNotExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestDropTestShardSet(runtime, ++txId, "/MyRoot", "MyTestShardSet", {{NKikimrScheme::StatusPathDoesNotExist, "error: path hasn't been resolved"}});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateExistingTestShardSet) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet"), {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/MyTestShardSet", false, NLs::PathExist);

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet"), {NKikimrScheme::StatusAlreadyExists});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AsyncCreateDifferentTestShardSets) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        AsyncCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("TestShardSet1"));
        AsyncCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("TestShardSet2"));

        TestModificationResult(runtime, txId - 1);
        TestModificationResult(runtime, txId);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestLs(runtime, "/MyRoot/TestShardSet1", false, NLs::PathExist);
        TestLs(runtime, "/MyRoot/TestShardSet2", false, NLs::PathExist);
    }

    Y_UNIT_TEST(AsyncCreateSameTestShardSet) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        AsyncCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet"));
        AsyncCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet"));

        TestModificationResults(runtime, txId - 1, {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusAlreadyExists});
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusAlreadyExists});
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestLs(runtime, "/MyRoot/MyTestShardSet", false, NLs::PathExist);
    }

    Y_UNIT_TEST(ReadOnlyMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        SetSchemeshardReadOnlyMode(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet"), {NKikimrScheme::StatusReadOnly});
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/MyTestShardSet", false, NLs::PathNotExist);

        SetSchemeshardReadOnlyMode(runtime, false);
        sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet"), {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/MyTestShardSet", false, NLs::PathExist);
    }

    Y_UNIT_TEST(EmptyName) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", R"(
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

    Y_UNIT_TEST(DescribeTestShardSet) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet"), {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        auto describeResult = DescribePath(runtime, "/MyRoot/MyTestShardSet");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsTestShardSet});

        const auto& pathDesc = describeResult.GetPathDescription();
        UNIT_ASSERT(pathDesc.HasTestShardSetDescription());
        const auto& desc = pathDesc.GetTestShardSetDescription();
        UNIT_ASSERT_VALUES_EQUAL(desc.GetName(), "MyTestShardSet");
        UNIT_ASSERT_VALUES_EQUAL(desc.TabletIdsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetVersion(), 1);
    }

    Y_UNIT_TEST(CreateMultipleShards) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("MyTestShardSet", 5), {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        auto describeResult = DescribePath(runtime, "/MyRoot/MyTestShardSet");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsTestShardSet});

        const auto& desc = describeResult.GetPathDescription().GetTestShardSetDescription();
        UNIT_ASSERT_VALUES_EQUAL(desc.TabletIdsSize(), 5);
        for (size_t i = 0; i < desc.TabletIdsSize(); ++i) {
            UNIT_ASSERT(desc.GetTabletIds(i) != 0);
        }
    }

    Y_UNIT_TEST(ZeroCount) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", R"(
                Name: "MyTestShardSet"
                Count: 0
                StorageConfig {
                }
                CmdInitialize {
                    MaxDataBytes: 1000
                }
            )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(NoCmdInitialize) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", R"(
                Name: "MyTestShardSet"
                Count: 1
                StorageConfig {
                }
            )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(NoStorageConfigUsesDefault) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", R"(
                Name: "MyTestShardSet"
                Count: 1
                CmdInitialize {
                    MaxDataBytes: 1000
                }
            )", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        auto describeResult = DescribePath(runtime, "/MyRoot/MyTestShardSet");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsTestShardSet});
    }

    Y_UNIT_TEST(CreateInSubdomain) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TEST_SHARD, NActors::NLog::PRI_DEBUG);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(
                Name: "SubDomain"
            )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot/SubDomain",
            CreateTestShardSetConfig("MyTestShardSet"), {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        auto describeResult = DescribePath(runtime, "/MyRoot/SubDomain/MyTestShardSet");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsTestShardSet});
    }
}

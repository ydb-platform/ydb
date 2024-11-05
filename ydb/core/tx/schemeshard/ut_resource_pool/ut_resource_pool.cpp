#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;


Y_UNIT_TEST_SUITE(TResourcePoolTest) {
    Y_UNIT_TEST(CreateResourcePool) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);

        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "MyResourcePool"
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool", false, NLs::PathExist);
    }

    Y_UNIT_TEST(CreateResourcePoolWithProperties) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);

        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "MyResourcePool"
                Properties {
                    Properties {
                        key: "concurrent_query_limit",
                        value: "10"
                    }
                    Properties {
                        key: "query_cancel_after_seconds",
                        value: "60"
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        NKikimrSchemeOp::TResourcePoolProperties properties;
        properties.MutableProperties()->insert({"concurrent_query_limit", "10"});
        properties.MutableProperties()->insert({"query_cancel_after_seconds", "60"});

        auto describeResult =  DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool");
        TestDescribeResult(describeResult, {NLs::PathExist});
        UNIT_ASSERT(describeResult.GetPathDescription().HasResourcePoolDescription());
        const auto& resourcePoolDescription = describeResult.GetPathDescription().GetResourcePoolDescription();
        UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetName(), "MyResourcePool");
        UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetProperties().DebugString(), properties.DebugString());
    }

    Y_UNIT_TEST(DropResourcePool) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);

        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "MyResourcePool"
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool", false, NLs::PathExist);

        TestDropResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", "MyResourcePool");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(DropResourcePoolTwice) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);

        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
            Name: "MyResourcePool"
        )");
        env.TestWaitNotification(runtime, txId);

        AsyncDropResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", "MyResourcePool");
        AsyncDropResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", "MyResourcePool");
        TestModificationResult(runtime, txId - 1);

        auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>();
        UNIT_ASSERT(ev);

        const auto& record = ev->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusMultipleModifications);
        UNIT_ASSERT_VALUES_EQUAL(record.GetPathDropTxId(), txId - 1);

        env.TestWaitNotification(runtime, txId - 1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool"), {
            NLs::PathNotExist
        });
    }

    Y_UNIT_TEST(ParallelCreateResourcePool) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        AsyncMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        AsyncCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "MyResourcePool1"
            )");
        AsyncCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "MyResourcePool2"
            )");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        TestDescribe(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool1");
        TestDescribe(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool2");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools"),
                           {NLs::PathVersionEqual(7)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool1"),
                           {NLs::PathVersionEqual(2)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool2"),
                           {NLs::PathVersionEqual(2)});
    }

    Y_UNIT_TEST(ParallelCreateSameResourcePool) {
        using ESts = NKikimrScheme::EStatus;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TString resourcePoolConfig = R"(
                Name: "NilNoviSubLuna"
            )";

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);

        AsyncCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", resourcePoolConfig);
        AsyncCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", resourcePoolConfig);
        AsyncCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", resourcePoolConfig);

        ui64 sts[3];
        sts[0] = TestModificationResults(runtime, txId-2, {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});
        sts[1] = TestModificationResults(runtime, txId-1, {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});
        sts[2] = TestModificationResults(runtime, txId,   {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});

        for (ui32 i=0; i<3; ++i) {
            if (sts[i] == ESts::StatusAlreadyExists) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/NilNoviSubLuna"),
                                   {NLs::Finished,
                                    NLs::IsResourcePool});
            }

            if (sts[i] == ESts::StatusMultipleModifications) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/NilNoviSubLuna"),
                                   {NLs::Finished,
                                    NLs::IsResourcePool});
            }
        }

        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/NilNoviSubLuna"),
                           {NLs::Finished,
                            NLs::IsResourcePool,
                            NLs::PathVersionEqual(2)});

        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", resourcePoolConfig, {ESts::StatusAlreadyExists});
    }

    Y_UNIT_TEST(ReadOnlyMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);

        AsyncMkDir(runtime, ++txId, "/MyRoot", "SubDirA");
        AsyncCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "MyResourcePool"
            )");

        // Set ReadOnly
        SetSchemeshardReadOnlyMode(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Verify that table creation successfully finished
        env.TestWaitNotification(runtime, txId);

        // Check that describe works
        TestDescribeResult(DescribePath(runtime, "/MyRoot/SubDirA"),
                           {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool"),
                           {NLs::Finished,
                            NLs::IsResourcePool});

        // Check that new modifications fail
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirBBBB", {NKikimrScheme::StatusReadOnly});
        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "MyResourcePool2"
            )", {NKikimrScheme::StatusReadOnly});

        // Disable ReadOnly
        SetSchemeshardReadOnlyMode(runtime, false);
        sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Check that modifications now work again
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirBBBB");
    }

    Y_UNIT_TEST(SchemeErrors) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);

        TestCreateResourcePool(runtime, ++txId, "/MyRoot", R"(
                Name: "AnotherDir/MyResourcePool"
            )", {{NKikimrScheme::StatusSchemeError, "Resource pools shoud be placed in /MyRoot/.metadata/workload_manager/pools"}});

        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "AnotherDir/MyResourcePool"
            )", {{NKikimrScheme::StatusSchemeError, "Resource pools shoud be placed in /MyRoot/.metadata/workload_manager/pools"}});

        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: ""
            )", {{NKikimrScheme::StatusSchemeError, "error: path part shouldn't be empty"}});
    }

    Y_UNIT_TEST(AlterResourcePool) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);

        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "MyResourcePool"
                Properties {
                    Properties {
                        key: "concurrent_query_limit",
                        value: "10"
                    }
                    Properties {
                        key: "query_count_limit",
                        value: "50"
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        NKikimrSchemeOp::TResourcePoolProperties properties;
        properties.MutableProperties()->insert({"concurrent_query_limit", "10"});
        properties.MutableProperties()->insert({"query_count_limit", "50"});

        {
            auto describeResult =  DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasResourcePoolDescription());
            const auto& resourcePoolDescription = describeResult.GetPathDescription().GetResourcePoolDescription();
            UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetName(), "MyResourcePool");
            UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetVersion(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetProperties().DebugString(), properties.DebugString());
        }

        TestAlterResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "MyResourcePool"
                Properties {
                    Properties {
                        key: "concurrent_query_limit",
                        value: "20"
                    }
                    Properties {
                        key: "query_cancel_after_seconds",
                        value: "60"
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        properties.MutableProperties()->find("concurrent_query_limit")->second = "20";
        properties.MutableProperties()->insert({"query_cancel_after_seconds", "60"});

        {
            auto describeResult =  DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasResourcePoolDescription());
            const auto& resourcePoolDescription = describeResult.GetPathDescription().GetResourcePoolDescription();
            UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetName(), "MyResourcePool");
            UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetVersion(), 2);
            UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetProperties().DebugString(), properties.DebugString());
        }
    }

    Y_UNIT_TEST(AlterResourcePoolShouldFailIfSuchEntityNotExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);

        TestAlterResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
                Name: "MyResourcePool"
                Properties {
                    Properties {
                        key: "concurrent_query_limit",
                        value: "20"
                    }
                }
            )", {NKikimrScheme::StatusPathDoesNotExist});
    }
}

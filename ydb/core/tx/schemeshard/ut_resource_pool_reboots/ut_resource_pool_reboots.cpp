#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;


Y_UNIT_TEST_SUITE(TResourcePoolTestReboots) {
    Y_UNIT_TEST(CreateResourcePoolWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncMkDir(runtime, ++t.TxId, "/MyRoot", ".metadata/workload_manager/pools");

            AsyncCreateResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", R"(
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
                )");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});

            NKikimrSchemeOp::TResourcePoolProperties properties;
            properties.MutableProperties()->insert({"concurrent_query_limit", "10"});
            properties.MutableProperties()->insert({"query_count_limit", "50"});

            {
                TInactiveZone inactive(activeZone);
                auto describeResult =  DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/MyResourcePool");
                TestDescribeResult(describeResult, {NLs::Finished});

                UNIT_ASSERT(describeResult.GetPathDescription().HasResourcePoolDescription());
                const auto& resourcePoolDescription = describeResult.GetPathDescription().GetResourcePoolDescription();
                UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetName(), "MyResourcePool");
                UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetVersion(), 1);
                UNIT_ASSERT_VALUES_EQUAL(resourcePoolDescription.GetProperties().DebugString(), properties.DebugString());
            }
        });
    }

    Y_UNIT_TEST(ParallelCreateDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".metadata/workload_manager/pools");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            AsyncCreateResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", R"(
                    Name: "DropMe"
                )");
            AsyncDropResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);


            TestDropResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/DropMe"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(SimpleDropResourcePoolWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".metadata/workload_manager/pools");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TInactiveZone inactive(activeZone);
                TestCreateResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", R"(
                        Name: "ResourcePool"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", "ResourcePool");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/ResourcePool"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(SimpleDropResourcePoolWithReboots2) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".metadata/workload_manager/pools");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", R"(
                        Name: "ResourcePool"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", "ResourcePool");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/ResourcePool"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(DropResourcePoolWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".metadata/workload_manager/pools");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateResourcePool(runtime, t.TxId, "/MyRoot/.metadata/workload_manager/pools", R"(
                        Name: "ResourcePool"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", "ResourcePool");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/ResourcePool"),
                                   {NLs::PathNotExist});

                TestCreateResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", R"(
                        Name: "ResourcePool"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", "ResourcePool");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/ResourcePool"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(CreateDroppedResourcePoolWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".metadata/workload_manager/pools");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", R"(
                        Name: "ResourcePool"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", "ResourcePool");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", R"(
                    Name: "ResourcePool"
                )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestDropResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", "ResourcePool");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }
        });
    }

    Y_UNIT_TEST(CreateDroppedResourcePoolAndDropWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".metadata/workload_manager/pools");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", R"(
                        Name: "ResourcePool"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", "ResourcePool");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", R"(
                        Name: "ResourcePool"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropResourcePool(runtime, ++t.TxId, "/MyRoot/.metadata/workload_manager/pools", "ResourcePool");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/ResourcePool"),
                                   {NLs::PathNotExist});
            }
        });
    }
}

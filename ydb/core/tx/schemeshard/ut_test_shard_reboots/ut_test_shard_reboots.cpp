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

Y_UNIT_TEST_SUITE(TTestShardSetTestReboots) {
    Y_UNIT_TEST(CreateTestShardSet) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "dir");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateTestShardSet(runtime, ++t.TxId, "/MyRoot/dir",
                CreateTestShardSetConfig("MyTestShardSet"));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/dir/MyTestShardSet", false, NLs::PathExist);
            }
        });
    }

    Y_UNIT_TEST(CreateTestShardSetMultipleShards) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TestCreateTestShardSet(runtime, ++t.TxId, "/MyRoot",
                CreateTestShardSetConfig("MyTestShardSet", 3));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                auto describeResult = DescribePath(runtime, "/MyRoot/MyTestShardSet");
                TestDescribeResult(describeResult, {NLs::Finished});
                const auto& pathDesc = describeResult.GetPathDescription();
                UNIT_ASSERT(pathDesc.HasTestShardSetDescription());
                UNIT_ASSERT_VALUES_EQUAL(pathDesc.GetTestShardSetDescription().TabletIdsSize(), 3);
            }
        });
    }

    Y_UNIT_TEST(DropTestShardSet) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTestShardSet(runtime, ++t.TxId, "/MyRoot",
                    CreateTestShardSetConfig("MyTestShardSet"));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestLs(runtime, "/MyRoot/MyTestShardSet", false, NLs::PathExist);
            }

            TestDropTestShardSet(runtime, ++t.TxId, "/MyRoot", "MyTestShardSet");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/MyTestShardSet", false, NLs::PathNotExist);
            }
        });
    }

    Y_UNIT_TEST(ParallelCreateDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncCreateTestShardSet(runtime, ++t.TxId, "/MyRoot",
                CreateTestShardSetConfig("DropMe"));

            AsyncDropTestShardSet(runtime, ++t.TxId, "/MyRoot", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId - 1);

            TestDropTestShardSet(runtime, ++t.TxId, "/MyRoot", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DropMe"), {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(DropAndRecreate) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTestShardSet(runtime, ++t.TxId, "/MyRoot",
                    CreateTestShardSetConfig("MyTestShardSet"));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTestShardSet(runtime, ++t.TxId, "/MyRoot", "MyTestShardSet");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateTestShardSet(runtime, ++t.TxId, "/MyRoot",
                CreateTestShardSetConfig("MyTestShardSet"));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/MyTestShardSet", false, NLs::PathExist);
            }
        });
    }

    Y_UNIT_TEST(DropRecreateAndDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTestShardSet(runtime, ++t.TxId, "/MyRoot",
                    CreateTestShardSetConfig("MyTestShardSet"));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTestShardSet(runtime, ++t.TxId, "/MyRoot", "MyTestShardSet");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTestShardSet(runtime, ++t.TxId, "/MyRoot",
                    CreateTestShardSetConfig("MyTestShardSet"));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropTestShardSet(runtime, ++t.TxId, "/MyRoot", "MyTestShardSet");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/MyTestShardSet"), {NLs::PathNotExist});
            }
        });
    }
}

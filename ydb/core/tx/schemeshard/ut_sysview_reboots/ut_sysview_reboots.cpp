#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>


using namespace NSchemeShardUT_Private;  // for helpers.h's Test*() methods

Y_UNIT_TEST_SUITE(TSchemeShardSysViewTestReboots) {
    Y_UNIT_TEST(CreateSysView) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TestCreateSysView(runtime, ++t.TxId, "/MyRoot/.sys",
                              Sprintf(R"(
                                 Name: "new_sys_view"
                                 Type: %d
                                )", static_cast<i32>(NKikimrSysView::ESysViewType::EPartitionStats)));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
                TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
            }
        });
    }

    Y_UNIT_TEST(DropSysView) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TestCreateSysView(runtime, ++t.TxId, "/MyRoot/.sys",
                                  Sprintf(R"(
                                     Name: "new_sys_view"
                                     Type: %d
                                    )", static_cast<i32>(NKikimrSysView::ESysViewType::EPartitionStats)));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathExist);
            }

            TestDropSysView(runtime, ++t.TxId, "/MyRoot/.sys", "new_sys_view");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathNotExist);
            }
        });
    }
}

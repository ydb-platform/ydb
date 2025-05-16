#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace NKikimr::NSchemeShard {
    extern bool isSysDirCreateAllowed;
}

namespace {

    using namespace NSchemeShardUT_Private;
    using NKikimrScheme::EStatus;

    class TSysDirCreateGuard : public TNonCopyable {
    public:
        TSysDirCreateGuard() {
            NKikimr::NSchemeShard::isSysDirCreateAllowed = true;
        }

        ~TSysDirCreateGuard() {
            NKikimr::NSchemeShard::isSysDirCreateAllowed = false;
        }
    };

}

Y_UNIT_TEST_SUITE(TSchemeShardSysViewTestReboots) {
    Y_UNIT_TEST(CreateSysViewWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TSysDirCreateGuard sysDirCreateGuard;
                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".sys");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateSysView(runtime, ++t.TxId, "/MyRoot/.sys",
                              R"(
                                 Name: "new_sys_view"
                                 Type: EPartitionStats
                                )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
                TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
            }
        });
    }

    Y_UNIT_TEST(DropSysViewWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TSysDirCreateGuard sysDirCreateGuard;
                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".sys");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestCreateSysView(runtime, ++t.TxId, "/MyRoot/.sys",
                                  R"(
                                     Name: "new_sys_view"
                                     Type: EPartitionStats
                                    )");
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
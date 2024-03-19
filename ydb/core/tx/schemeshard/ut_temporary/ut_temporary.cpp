#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

void Check(TTestActorRuntime& runtime, NLs::TCheckFunc func, const char* tableName = "TestTable") {
    TestDescribeResult(
        DescribePath(runtime, Sprintf("/MyRoot/%s", tableName)), {
            NLs::PathExist,
            NLs::Finished,
            func,
        }
    );
}

}

Y_UNIT_TEST_SUITE(TSchemeShardTemporaryTests) {
    Y_UNIT_TEST(AlterTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrConfig::TFeatureFlags features;
        features.SetEnableTempTables(true);
        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        *request->Record.MutableConfig()->MutableFeatureFlags() = features;
        SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "Timestamp" }
            KeyColumnNames: ["key"]
            Temporary: True
        )");

        env.TestWaitNotification(runtime, txId);

        Check(runtime, [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            UNIT_ASSERT(record.GetPathDescription().GetTable().GetTemporary());
        });

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            Temporary: False
        )");

        env.TestWaitNotification(runtime, txId);

        Check(runtime, [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            UNIT_ASSERT(!record.GetPathDescription().GetTable().GetTemporary());
        });
    }
}

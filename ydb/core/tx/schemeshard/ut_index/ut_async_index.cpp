#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TAsyncIndexTests) {
    Y_UNIT_TEST(CreateTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
              Type: EIndexTypeGlobalAsync
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndex"),
            {NLs::PathExist,
             NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalAsync),
             NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
             NLs::IndexKeys({"indexed"})});
    }

    Y_UNIT_TEST(OnlineBuild) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "indexed" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{
            "UserDefinedIndex", NKikimrSchemeOp::EIndexTypeGlobalAsync, {"indexed"}, {}
        });
        env.TestWaitNotification(runtime, txId);
    }
}

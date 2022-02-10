#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TAsyncIndexTests) {
    void CreateTable(bool enableAsyncIndexes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAsyncIndexes(enableAsyncIndexes));
        ui64 txId = 100;

        const auto status = enableAsyncIndexes
            ? NKikimrScheme::StatusAccepted
            : NKikimrScheme::StatusPreconditionFailed;

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
        )", {status});

        if (enableAsyncIndexes) {
            env.TestWaitNotification(runtime, txId);
        }
    }

    Y_UNIT_TEST(CreateTableShouldSucceed) {
        CreateTable(true);
    }

    Y_UNIT_TEST(CreateTableShouldFail) {
        CreateTable(false);
    }

    void OnlineBuild(bool enableAsyncIndexes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAsyncIndexes(enableAsyncIndexes));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "indexed" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const auto status = enableAsyncIndexes
            ? Ydb::StatusIds::SUCCESS
            : Ydb::StatusIds::UNSUPPORTED;

        TestBuilIndex(runtime,  ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{ 
            "UserDefinedIndex", NKikimrSchemeOp::EIndexTypeGlobalAsync, {"indexed"}, {}
        }, status);

        if (enableAsyncIndexes) {
            env.TestWaitNotification(runtime, txId);
        }
    }

    Y_UNIT_TEST(OnlineBuildShouldSucceed) {
        OnlineBuild(true);
    }

    Y_UNIT_TEST(OnlineBuildShouldFail) {
        OnlineBuild(false);
    }
}

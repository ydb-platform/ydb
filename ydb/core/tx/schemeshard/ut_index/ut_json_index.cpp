#include <ydb/core/base/path.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NTableIndex;

Y_UNIT_TEST_SUITE(TJsonIndexTests) {
    Y_UNIT_TEST(CreateTableWithJsonColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "table"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "data" Type: "Json" }
                Columns { Name: "other" Type: "String" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_json"
                KeyColumnNames: ["data"]
                Type: EIndexTypeGlobalJson
            }
        )");
        env.TestWaitNotification(runtime, txId);

        for (ui32 reboot = 0; reboot < 2; reboot++) {
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json"), {
                NLs::PathExist,
                NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalJson),
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                NLs::IndexKeys({"data"}),
                NLs::IndexDataColumns({}),
                NLs::ChildrenCount(1),
            });

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json/indexImplTable"), {
                NLs::PathExist,
                NLs::CheckColumns("indexImplTable",
                        { NTableIndex::NFulltext::TokenColumn, "id" }, {},
                        { NTableIndex::NFulltext::TokenColumn, "id" }, true) });

            Cerr << "Reboot SchemeShard.." << Endl;
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }
    }

    Y_UNIT_TEST(CreateTableWithJsonDocumentColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "table"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "data" Type: "JsonDocument" }
                Columns { Name: "other" Type: "String" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_json"
                KeyColumnNames: ["data"]
                Type: EIndexTypeGlobalJson
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalJson),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            NLs::IndexKeys({"data"}),
            NLs::IndexDataColumns({}),
        });
    }

    Y_UNIT_TEST(CreateTableWithUint32PK) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "table"
                Columns { Name: "id" Type: "Uint32" }
                Columns { Name: "data" Type: "Json" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_json"
                KeyColumnNames: ["data"]
                Type: EIndexTypeGlobalJson
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalJson),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
    }

    Y_UNIT_TEST(CreateTableWithInt32PK) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "table"
                Columns { Name: "id" Type: "Int32" }
                Columns { Name: "data" Type: "Json" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_json"
                KeyColumnNames: ["data"]
                Type: EIndexTypeGlobalJson
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalJson),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
    }

    Y_UNIT_TEST(CreateTableWithInt64PK) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "table"
                Columns { Name: "id" Type: "Int64" }
                Columns { Name: "data" Type: "Json" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_json"
                KeyColumnNames: ["data"]
                Type: EIndexTypeGlobalJson
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalJson),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
    }

    Y_UNIT_TEST(CreateTableMultipleKeyColumns) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "table"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "data1" Type: "Json" }
                Columns { Name: "data2" Type: "Json" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_json"
                KeyColumnNames: ["data1", "data2"]
                Type: EIndexTypeGlobalJson
            }
        )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json"), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateTableWithDataColumns) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "table"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "data" Type: "Json" }
                Columns { Name: "extra" Type: "String" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_json"
                KeyColumnNames: ["data"]
                DataColumnNames: ["extra"]
                Type: EIndexTypeGlobalJson
            }
        )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json"), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateTableWrongKeyColumnType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "table"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "data" Type: "String" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_json"
                KeyColumnNames: ["data"]
                Type: EIndexTypeGlobalJson
            }
        )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json"), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateTableMultiplePrimaryKeys) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "table"
                Columns { Name: "id1" Type: "Uint64" }
                Columns { Name: "id2" Type: "Uint64" }
                Columns { Name: "data" Type: "Json" }
                KeyColumnNames: ["id1", "id2"]
            }
            IndexDescription {
                Name: "idx_json"
                KeyColumnNames: ["data"]
                Type: EIndexTypeGlobalJson
            }
        )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json"), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateTableWrongPrimaryKeyType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "table"
                Columns { Name: "id" Type: "String" }
                Columns { Name: "data" Type: "Json" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "idx_json"
                KeyColumnNames: ["data"]
                Type: EIndexTypeGlobalJson
            }
        )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/idx_json"), {
            NLs::PathNotExist,
        });
    }
}

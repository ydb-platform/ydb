#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TDropNotNullIndex) {
    Y_UNIT_TEST_FLAG(BusyIndex, ImplementationOnly) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "Key" Type: "Uint64" }
                Columns { Name: "Value" Type: "Uint64" NotNull: true }
                Columns { Name: "Covered" Type: "Uint64" NotNull: true }
                KeyColumnNames: ["Key"]
            }
            IndexDescription { Name: "idx" KeyColumnNames: ["Value"] DataColumnNames: ["Covered"] Type: EIndexTypeGlobal }
        )");
        env.TestWaitNotification(runtime, txId);

        TBlockEvents<TEvDataShard::TEvProposeTransaction> proposals(runtime);
        const ui64 indexTxId = ++txId;
        if (ImplementationOnly) {
            // Start a standalone internal impl-table alter in a multi-transaction request.
            // The preceding indexed-table creation enables access to private paths without
            // altering the target index object, so only its impl table is busy.
            auto request = CreateIndexedTableRequest(indexTxId, "/MyRoot", R"(
                TableDescription {
                    Name: "Unrelated" Columns { Name: "Key" Type: "Uint64" }
                    Columns { Name: "Value" Type: "Uint64" } KeyColumnNames: ["Key"]
                }
                IndexDescription { Name: "idx" KeyColumnNames: ["Value"] }
            )");
            THolder<TEvSchemeShard::TEvModifySchemeTransaction> implAlter(
                InternalTransaction(AlterTableRequest(indexTxId, "/MyRoot/Table/idx", R"(
                    Name: "indexImplTable" Columns { Name: "Covered" NotNull: false }
                )")));
            *request->Record.AddTransaction() = implAlter->Record.GetTransaction(0);
            AsyncSend(runtime, TTestTxConfig::SchemeShard, request);
            TestModificationResults(runtime, indexTxId, {NKikimrScheme::StatusAccepted});
        } else {
            TestAlterTable(runtime, indexTxId, "/MyRoot/Table/idx", R"(
                Name: "indexImplTable"
                PartitionConfig { PartitioningPolicy { MinPartitionsCount: 2 } }
            )");
        }
        runtime.WaitFor("index alteration to reach the shard", [&] { return proposals.size(); });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/idx"), {
            NLs::CheckPathState(ImplementationOnly ? NKikimrSchemeOp::EPathStateNoChanges : NKikimrSchemeOp::EPathStateAlter),
        });
        const auto before = DescribePrivatePath(runtime, "/MyRoot/Table").GetPathDescription().GetTable();
        const auto indexBefore = DescribePrivatePath(runtime, "/MyRoot/Table/idx/indexImplTable").GetPathDescription().GetTable();
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table" Columns { Name: "Value" NotNull: false }
        )", {NKikimrScheme::StatusMultipleModifications});
        const auto after = DescribePrivatePath(runtime, "/MyRoot/Table").GetPathDescription().GetTable();
        const auto indexAfter = DescribePrivatePath(runtime, "/MyRoot/Table/idx/indexImplTable").GetPathDescription().GetTable();
        UNIT_ASSERT_VALUES_EQUAL(before.GetTableSchemaVersion(), after.GetTableSchemaVersion());
        UNIT_ASSERT_VALUES_EQUAL(indexBefore.GetTableSchemaVersion(), indexAfter.GetTableSchemaVersion());
        for (const auto* table : {&after, &indexAfter}) {
            bool found = false;
            for (const auto& column : table->GetColumns()) {
                if (column.GetName() == "Value") {
                    UNIT_ASSERT(column.GetNotNull());
                    found = true;
                }
            }
            UNIT_ASSERT(found);
        }
        proposals.Stop().Unblock();
        env.TestWaitNotification(runtime, indexTxId);
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table" Columns { Name: "Value" NotNull: false }
        )");
        env.TestWaitNotification(runtime, txId);
        for (const TString& path : {TString("/MyRoot/Table"), TString("/MyRoot/Table/idx/indexImplTable")}) {
            const auto table = DescribePrivatePath(runtime, path).GetPathDescription().GetTable();
            for (const auto& column : table.GetColumns()) {
                if (column.GetName() == "Value") {
                    UNIT_ASSERT(!column.GetNotNull());
                }
            }
        }
    }
}

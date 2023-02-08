#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TExternalTableTest) {
    Y_UNIT_TEST(CreateExternalTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExternalTable(runtime, txId++, "/MyRoot",
                            R"(Name: "external_table1")",
                            {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, 100);

        TestLs(runtime, "/MyRoot/external_table1", false, NLs::PathExist);
    }

    Y_UNIT_TEST(DropExternalTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExternalTable(runtime, txId++, "/MyRoot",
                            R"(Name: "external_table1")",
                            {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, 100);

        TestLs(runtime, "/MyRoot/external_table1", false, NLs::PathExist);

        TestDropExternalTable(runtime, ++txId, "/MyRoot", "external_table1");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/external_table1", false, NLs::PathNotExist);
    }

    using TRuntimeTxFn = std::function<void(TTestBasicRuntime&, ui64)>;

    void DropTwice(const TString& path, TRuntimeTxFn createFn, TRuntimeTxFn dropFn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        createFn(runtime, ++txId);
        env.TestWaitNotification(runtime, txId);

        dropFn(runtime, ++txId);
        dropFn(runtime, ++txId);
        TestModificationResult(runtime, txId - 1);

        auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>();
        UNIT_ASSERT(ev);

        const auto& record = ev->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusMultipleModifications);
        UNIT_ASSERT_VALUES_EQUAL(record.GetPathDropTxId(), txId - 1);

        env.TestWaitNotification(runtime, txId - 1);
        TestDescribeResult(DescribePath(runtime, path), {
            NLs::PathNotExist
        });
    }

    Y_UNIT_TEST(DropTableTwice) {
        auto createFn = [](TTestBasicRuntime& runtime, ui64 txId) {
            TestCreateExternalTable(runtime, txId, "/MyRoot", R"(
                  Name: "ExternalTable"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value" Type: "Utf8" }
            )");
        };

        auto dropFn = [](TTestBasicRuntime& runtime, ui64 txId) {
            AsyncDropExternalTable(runtime, txId, "/MyRoot", "ExternalTable");
        };

        DropTwice("/MyRoot/ExternalTable", createFn, dropFn);
    }

    Y_UNIT_TEST(ParallelCreateExternalTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot/DirA",
                                R"(Name: "ExternalTable1"
                                          Columns { Name: "RowId"      Type: "Uint64"}
                                          Columns { Name: "Value"      Type: "Utf8"})");
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot/DirA",
                                R"(Name: "ExternalTable2"
                                   Columns { Name: "key1"       Type: "Uint32"}
                                   Columns { Name: "key2"       Type: "Utf8"}
                                   Columns { Name: "RowId"      Type: "Uint64"}
                                   Columns { Name: "Value"      Type: "Utf8"})");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        TestDescribe(runtime, "/MyRoot/DirA/ExternalTable1");
        TestDescribe(runtime, "/MyRoot/DirA/ExternalTable2");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(7)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/ExternalTable1"),
                           {NLs::PathVersionEqual(2)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/ExternalTable2"),
                           {NLs::PathVersionEqual(2)});
    }

    Y_UNIT_TEST(ParallelCreateSameExternalTable) {
        using ESts = NKikimrScheme::EStatus;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TString tableConfig = R"(Name: "NilNoviSubLuna"
            Columns { Name: "key"        Type: "Uint64"}
            Columns { Name: "value"      Type: "Uint64"})";

        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot", tableConfig);
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot", tableConfig);
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot", tableConfig);

        ui64 sts[3];
        sts[0] = TestModificationResults(runtime, txId-2, {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});
        sts[1] = TestModificationResults(runtime, txId-1, {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});
        sts[2] = TestModificationResults(runtime, txId,   {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});

        for (ui32 i=0; i<3; ++i) {
            if (sts[i] == ESts::StatusAlreadyExists) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                                   {NLs::Finished,
                                    NLs::IsExternalTable});
            }

            if (sts[i] == ESts::StatusMultipleModifications) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                                   {NLs::Finished,
                                    NLs::IsExternalTable});
            }
        }

        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                           {NLs::Finished,
                            NLs::IsExternalTable,
                            NLs::PathVersionEqual(2)});

        TestCreateExternalTable(runtime, ++txId, "/MyRoot", tableConfig, {ESts::StatusAlreadyExists});

    }


    Y_UNIT_TEST(ReadOnlyMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "SubDirA");
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot",
                                        R"(Name: "ExternalTable1"
                                          Columns { Name: "RowId"      Type: "Uint64"}
                                          Columns { Name: "Value"      Type: "Utf8"})");
        // Set ReadOnly
        SetSchemeshardReadOnlyMode(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Verify that table creation successfully finished
        env.TestWaitNotification(runtime, txId);

        // Check that describe works
        TestDescribeResult(DescribePath(runtime, "/MyRoot/SubDirA"),
                           {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalTable1"),
                           {NLs::Finished,
                            NLs::IsExternalTable});

        // Check that new modifications fail
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirBBBB", {NKikimrScheme::StatusReadOnly});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot",
                                R"(Name: "ExternalTable1"
                                          Columns { Name: "RowId"      Type: "Uint64"}
                                          Columns { Name: "Value"      Type: "Utf8"})",
                                {NKikimrScheme::StatusReadOnly});

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

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA",
                R"(Name: "Table2"
                            Columns { Name: "RowId"      Type: "BlaBlaType"})",
                        {NKikimrScheme::StatusSchemeError});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA",
                R"(Name: "Table2"
                          Columns { Name: ""      Type: "Uint64"})",
                {NKikimrScheme::StatusSchemeError});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA",
                R"(Name: "Table2"
                          Columns { Name: "RowId"      TypeId: 27})",
                {NKikimrScheme::StatusSchemeError});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA",
                R"(Name: "Table2"
                            Columns { Name: "RowId" })",
            {NKikimrScheme::StatusSchemeError});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA",
                R"(Name: "Table2"
                        Columns { Name: "RowId" Type: "Uint64" Id: 2}
                        Columns { Name: "RowId2" Type: "Uint64" Id: 2 })",
            {NKikimrScheme::StatusSchemeError});
    }
}

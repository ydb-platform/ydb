#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardConsistentCopyTablesTest) {
    void SetupLogging(TTestActorRuntimeBase& runtime) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    }

    // Priority 1 Test 1: Regular consistent copy with global sync index
    // This test would have caught the OmitIndexes bug
    Y_UNIT_TEST(ConsistentCopyTableWithGlobalSyncIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        // 1. Create table with global sync index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // 2. Perform consistent copy (NOT backup - this is the critical test case)
        TestConsistentCopyTables(runtime, ++txId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/TableWithIndex"
                DstPath: "/MyRoot/TableWithIndexCopy"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // 3. Verify ALL components exist
        // Main table
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TableWithIndexCopy"),
                          {NLs::PathExist});

        // Index structure
        auto indexDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithIndexCopy/ValueIndex", true, true);
        UNIT_ASSERT(indexDesc.GetPathDescription().HasTableIndex());
        UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPathDescription().GetTableIndex().GetState(),
                                NKikimrSchemeOp::EIndexStateReady);

        // CRITICAL: Verify index impl table exists (THIS WOULD HAVE FAILED WITH THE BUG)
        UNIT_ASSERT_C(indexDesc.GetPathDescription().ChildrenSize() == 1,
                     "Index should have exactly one impl table child");

        TString indexImplTableName = indexDesc.GetPathDescription().GetChildren(0).GetName();
        Cerr << "Index impl table name: " << indexImplTableName << Endl;

        auto indexImplTableDesc = DescribePrivatePath(runtime,
            "/MyRoot/TableWithIndexCopy/ValueIndex/" + indexImplTableName, true, true);
        UNIT_ASSERT_C(indexImplTableDesc.GetPathDescription().HasTable(),
                     "Index impl table should exist - this is what the bug broke!");
    }

    // Priority 1 Test 2: OmitIndexes flag should be respected
    Y_UNIT_TEST(ConsistentCopyWithOmitIndexesTrueSkipsIndexes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        // 1. Create table with global sync index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // 2. Copy with OmitIndexes=true (explicit user request)
        TestConsistentCopyTables(runtime, ++txId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/TableWithIndex"
                DstPath: "/MyRoot/TableCopy"
                OmitIndexes: true
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // 3. Verify main table exists
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TableCopy"),
                          {NLs::PathExist});

        // 4. Verify index does NOT exist (user requested to omit it)
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TableCopy/ValueIndex"),
                          {NLs::PathNotExist});
    }

    // Priority 1 Test 3: Incremental backup path should still work
    // (Ensure fix didn't break the use case it was designed for)
    Y_UNIT_TEST(IncrementalBackupIndexesContinuesToWork) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        // Prepare backup directories
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);

        // Create incremental backup collection
        TString collectionSettings = R"(
            Name: "TestCollection"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithIndex"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )";

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create table with one global sync index
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Execute full backup
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/TestCollection")");
        env.TestWaitNotification(runtime, txId);

        // Verify CDC stream exists on main table
        auto mainTableDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithIndex", true, true);
        UNIT_ASSERT(mainTableDesc.GetPathDescription().HasTable());

        const auto& tableDesc = mainTableDesc.GetPathDescription().GetTable();
        bool foundMainTableCdc = false;

        for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
            const auto& cdcStream = tableDesc.GetCdcStreams(i);
            if (cdcStream.GetName().EndsWith("_continuousBackupImpl")) {
                foundMainTableCdc = true;
                break;
            }
        }
        UNIT_ASSERT_C(foundMainTableCdc, "Main table should have CDC stream for incremental backup");

        // Verify CDC stream exists on index implementation table
        auto indexDesc = DescribePrivatePath(runtime, "/MyRoot/TableWithIndex/ValueIndex", true, true);
        UNIT_ASSERT(indexDesc.GetPathDescription().HasTableIndex());

        // Get index implementation table
        UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPathDescription().ChildrenSize(), 1);
        TString indexImplTableName = indexDesc.GetPathDescription().GetChildren(0).GetName();

        auto indexImplTableDesc = DescribePrivatePath(runtime,
            "/MyRoot/TableWithIndex/ValueIndex/" + indexImplTableName, true, true);
        UNIT_ASSERT(indexImplTableDesc.GetPathDescription().HasTable());

        const auto& indexTableDesc = indexImplTableDesc.GetPathDescription().GetTable();
        bool foundIndexCdc = false;

        for (size_t i = 0; i < indexTableDesc.CdcStreamsSize(); ++i) {
            const auto& cdcStream = indexTableDesc.GetCdcStreams(i);
            if (cdcStream.GetName().EndsWith("_continuousBackupImpl")) {
                foundIndexCdc = true;
                break;
            }
        }
        UNIT_ASSERT_C(foundIndexCdc,
                     "Index impl table should have CDC stream for incremental backup");
    }

    // Priority 2 Test 1: Multiple indexes
    Y_UNIT_TEST(ConsistentCopyTableWithMultipleIndexes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        // 1. Create table with multiple global indexes
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithMultipleIndexes"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value1" Type: "Utf8" }
                Columns { Name: "value2" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex1"
                KeyColumnNames: ["value1"]
                Type: EIndexTypeGlobal
            }
            IndexDescription {
                Name: "ValueIndex2"
                KeyColumnNames: ["value2"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // 2. Perform consistent copy
        TestConsistentCopyTables(runtime, ++txId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/TableWithMultipleIndexes"
                DstPath: "/MyRoot/TableCopy"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // 3. Verify both indexes and their impl tables exist
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TableCopy"),
                          {NLs::PathExist});

        // Check first index
        auto index1Desc = DescribePrivatePath(runtime, "/MyRoot/TableCopy/ValueIndex1", true, true);
        UNIT_ASSERT(index1Desc.GetPathDescription().HasTableIndex());
        UNIT_ASSERT_VALUES_EQUAL(index1Desc.GetPathDescription().ChildrenSize(), 1);
        TString implTable1Name = index1Desc.GetPathDescription().GetChildren(0).GetName();
        TestDescribeResult(DescribePrivatePath(runtime,
            "/MyRoot/TableCopy/ValueIndex1/" + implTable1Name),
            {NLs::PathExist});

        // Check second index
        auto index2Desc = DescribePrivatePath(runtime, "/MyRoot/TableCopy/ValueIndex2", true, true);
        UNIT_ASSERT(index2Desc.GetPathDescription().HasTableIndex());
        UNIT_ASSERT_VALUES_EQUAL(index2Desc.GetPathDescription().ChildrenSize(), 1);
        TString implTable2Name = index2Desc.GetPathDescription().GetChildren(0).GetName();
        TestDescribeResult(DescribePrivatePath(runtime,
            "/MyRoot/TableCopy/ValueIndex2/" + implTable2Name),
            {NLs::PathExist});
    }
}

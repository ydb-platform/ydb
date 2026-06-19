#include <ydb/core/base/table_index.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/local_indexes.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr;
using NKikimrSchemeOp::EIndexType;

Y_UNIT_TEST_SUITE(TSchemeShardConsistentCopyTablesTest) {
    void SetupLogging(TTestActorRuntimeBase& runtime) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    }

    void ConsistentCopyTableWithIndex(
        const TString& indexDescription,
        NKikimrSchemeOp::EIndexType expectedIndexType,
        const TVector<TString>& indexKeyColumns)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "embedding" Type: "String" }
                Columns { Name: "prefix" Type: "String" }
                Columns { Name: "value" Type: "Utf8" }
                Columns { Name: "json" Type: "Json" }
                KeyColumnNames: ["key"]
            }
            %s
        )", indexDescription.c_str()));
        env.TestWaitNotification(runtime, txId);

        TestConsistentCopyTables(runtime, ++txId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/TableWithIndex"
                DstPath: "/MyRoot/TableCopy"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TableCopy"),
                          {NLs::PathExist, NLs::IsTable, NLs::IndexesCount(1)});

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TableCopy/index", true, true),
                          {NLs::PathExist,
                           NLs::IndexType(expectedIndexType),
                           NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady),
                           NLs::IndexKeys(indexKeyColumns)});

        for (const auto& implTable : NTableIndex::GetImplTables(expectedIndexType, indexKeyColumns)) {
            TestDescribeResult(
                DescribePrivatePath(runtime,
                    TString::Join("/MyRoot/TableCopy/index/", implTable), true, true),
                {NLs::PathExist, NLs::IsTable});
        }
    }

    Y_UNIT_TEST(ConsistentCopyTableWithGlobalSyncIndex) {
        ConsistentCopyTableWithIndex(R"(
            IndexDescription {
                Name: "index"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )",
        NKikimrSchemeOp::EIndexTypeGlobal,
        {"value"});
    }

    Y_UNIT_TEST(ConsistentCopyTableWithGlobalAsyncIndex) {
        ConsistentCopyTableWithIndex(R"(
            IndexDescription {
                Name: "index"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobalAsync
            }
        )",
        NKikimrSchemeOp::EIndexTypeGlobalAsync,
        {"value"});
    }

    Y_UNIT_TEST(ConsistentCopyTableWithGlobalUniqueIndex) {
        ConsistentCopyTableWithIndex(R"(
            IndexDescription {
                Name: "index"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobalUnique
            }
        )",
        NKikimrSchemeOp::EIndexTypeGlobalUnique,
        {"value"});
    }

    Y_UNIT_TEST(ConsistentCopyTableWithGlobalVectorKmeansTreeIndex) {
        ConsistentCopyTableWithIndex(R"(
            IndexDescription {
                Name: "index"
                KeyColumnNames: ["embedding"]
                Type: EIndexTypeGlobalVectorKmeansTree
                VectorIndexKmeansTreeDescription {
                    Settings {
                        settings {
                            metric: DISTANCE_COSINE
                            vector_type: VECTOR_TYPE_FLOAT
                            vector_dimension: 1024
                        }
                        clusters: 4
                        levels: 5
                    }
                }
            }
        )",
        NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree,
        {"embedding"});
    }

    Y_UNIT_TEST(ConsistentCopyTableWithGlobalVectorKmeansTreePrefixIndex) {
        ConsistentCopyTableWithIndex(R"(
            IndexDescription {
                Name: "index"
                KeyColumnNames: ["prefix", "embedding"]
                Type: EIndexTypeGlobalVectorKmeansTree
                VectorIndexKmeansTreeDescription {
                    Settings {
                        settings {
                            metric: DISTANCE_COSINE
                            vector_type: VECTOR_TYPE_FLOAT
                            vector_dimension: 1024
                        }
                        clusters: 4
                        levels: 5
                    }
                }
            }
        )",
        NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree,
        {"prefix", "embedding"});
    }

    Y_UNIT_TEST(ConsistentCopyTableWithGlobalFulltextPlainIndex) {
        ConsistentCopyTableWithIndex(R"(
            IndexDescription {
                Name: "index"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobalFulltextPlain
                FulltextIndexDescription {
                    Settings {
                        columns: {
                            column: "value"
                            analyzers: {
                                tokenizer: STANDARD
                                use_filter_lowercase: true
                            }
                        }
                    }
                }
            }
        )",
        NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain,
        {"value"});
    }

    Y_UNIT_TEST(ConsistentCopyTableWithGlobalJsonIndex) {
        ConsistentCopyTableWithIndex(R"(
            IndexDescription {
                Name: "index"
                KeyColumnNames: ["json"]
                Type: EIndexTypeGlobalJson
            }
        )",
        NKikimrSchemeOp::EIndexTypeGlobalJson,
        {"json"});
    }

    Y_UNIT_TEST(ConsistentCopyTableWithGlobalFulltextRelevanceIndex) {
        ConsistentCopyTableWithIndex(R"(
            IndexDescription {
                Name: "index"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobalFulltextRelevance
                FulltextIndexDescription {
                    Settings {
                        columns: {
                            column: "value"
                            analyzers: {
                                tokenizer: STANDARD
                                use_filter_lowercase: true
                            }
                        }
                    }
                }
            }
        )",
        NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance,
        {"value"});
    }

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
                Columns { Name: "value3" Type: "Utf8" }
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
                Type: EIndexTypeGlobalAsync
            }
            IndexDescription {
                Name: "ValueIndex3"
                KeyColumnNames: ["value3"]
                Type: EIndexTypeGlobalUnique
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

        // Check indexes
        NKikimrSchemeOp::EIndexType expectedType[3] = {EIndexType::EIndexTypeGlobal,
            EIndexType::EIndexTypeGlobalAsync, EIndexType::EIndexTypeGlobalUnique};
        size_t i = 0;
        for (auto idx: {"ValueIndex1", "ValueIndex2", "ValueIndex3"}) {
            auto indexDesc = DescribePrivatePath(runtime, TString::Join("/MyRoot/TableCopy/", idx), true, true);
            UNIT_ASSERT(indexDesc.GetPathDescription().HasTableIndex());
            auto tableIndex = indexDesc.GetPathDescription().GetTableIndex();
            UNIT_ASSERT_VALUES_EQUAL(tableIndex.GetState(), NKikimrSchemeOp::EIndexStateReady);
            UNIT_ASSERT_VALUES_EQUAL(tableIndex.GetType(), expectedType[i]);
            UNIT_ASSERT_VALUES_EQUAL(tableIndex.KeyColumnNamesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(tableIndex.GetKeyColumnNames(0), Sprintf("value%d", i+1));
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPathDescription().ChildrenSize(), 1);
            TestDescribeResult(DescribePrivatePath(runtime,
                TString::Join("/MyRoot/TableCopy/", idx, "/", NTableIndex::ImplTable)),
                {NLs::PathExist});
            i++;
        }
    }

    // After a real vector index build, the transient 'indexImplPostingTable<N>build' intermediates are dropped
    // but their entries linger in the parent index's Children map.
    // CreateConsistentCopyTables should not fail on these stale references.
    Y_UNIT_TEST(ConsistentCopyTableAfterVectorIndexBuild) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/Table", 0, 0, 200);

        const ui64 buildIndexTx = ++txId;
        TestBuildVectorIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/Table", "index1", {"embedding"});
        env.TestWaitNotification(runtime, buildIndexTx);

        TestConsistentCopyTables(runtime, ++txId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table"
                DstPath: "/MyRoot/TableCopy"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TableCopy"),
            {NLs::PathExist, NLs::IsTable, NLs::IndexesCount(1)});

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/TableCopy/index1", true, true), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
            NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady),
        });
    }

    // Priority 1 Test 3: Consistent copy of column table with local bloom indexes
    Y_UNIT_TEST(ConsistentCopyColumnTableWithLocalBloomIndexes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetupLogging(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableLocalIndexAsSchemeObject(true);
        runtime.GetAppData().FeatureFlags.SetEnableColumnTablesBackup(true);

        // 1. Create column table with local bloom indexes
        TestCreateColumnTable(runtime, ++txId, "/MyRoot",
            NLocalIndexes::OlapTableWithBloomAndNgramIndexes("ColumnTableWithLocalIndexes"));
        env.TestWaitNotification(runtime, txId);

        // 2. Perform consistent copy with backup flag (required for column tables)
        TestConsistentCopyTables(runtime, ++txId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/ColumnTableWithLocalIndexes"
                DstPath: "/MyRoot/ColumnTableWithLocalIndexesCopy"
                IsBackup: true
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // 3. Verify the copied table and both bloom indexes are ready scheme objects
        NLocalIndexes::CheckOlapTableWithBloomAndNgramIndexesReady(runtime, "/MyRoot/ColumnTableWithLocalIndexesCopy");
    }
}

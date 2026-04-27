#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/index/index_utils.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>


using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NTableIndex;
using namespace NKikimr::NTableIndex::NKMeans;

Y_UNIT_TEST_SUITE(TVectorIndexTests) {
    Y_UNIT_TEST(CreateTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "embedding" Type: "String" }
              Columns { Name: "covered" Type: "String" }
              Columns { Name: "another" Type: "String" }
              KeyColumnNames: ["id"]
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["embedding"]
              DataColumnNames: ["covered"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 }, clusters: 4, levels: 5 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector"),
            { NLs::PathExist,
              NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
              NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
              NLs::IndexKeys({"embedding"}),
              NLs::IndexDataColumns({"covered"}),
              NLs::KMeansTreeDescription(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                                          Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
                                          1024,
                                          4,
                                          5
                                          ),
            });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplLevelTable"),
            { NLs::PathExist,
              NLs::CheckColumns(LevelTable, {ParentColumn, IdColumn, CentroidColumn}, {}, {ParentColumn, IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplPostingTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PostingTable, {ParentColumn, "id", "covered"}, {}, {ParentColumn, "id"}, true) });


        TVector<ui64> dropTxIds;
        TestDropTable(runtime, dropTxIds.emplace_back(++txId), "/MyRoot", "vectors");
        env.TestWaitNotification(runtime, dropTxIds);
    }

    Y_UNIT_TEST(CreateTablePrefix) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "embedding" Type: "String" }
              Columns { Name: "prefix" Type: "String" }
              KeyColumnNames: ["id"]
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["prefix", "embedding"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 }, clusters: 4, levels: 5 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector"),
            { NLs::PathExist,
              NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
              NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
              NLs::IndexKeys({"prefix", "embedding"}),
              NLs::IndexDataColumns({}),
              NLs::KMeansTreeDescription(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                                          Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
                                          1024,
                                          4,
                                          5
                                          ),
            });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplPrefixTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PrefixTable, {"prefix", IdColumn}, {}, {"prefix", IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplLevelTable"),
            { NLs::PathExist,
              NLs::CheckColumns(LevelTable, {ParentColumn, IdColumn, CentroidColumn}, {}, {ParentColumn, IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplPostingTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PostingTable, {ParentColumn, "id"}, {}, {ParentColumn, "id"}, true) });


        TVector<ui64> dropTxIds;
        TestDropTable(runtime, dropTxIds.emplace_back(++txId), "/MyRoot", "vectors");
        env.TestWaitNotification(runtime, dropTxIds);
    }

    Y_UNIT_TEST(CreateTablePrefixCovering) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "embedding" Type: "String" }
              Columns { Name: "covered" Type: "String" }
              Columns { Name: "prefix" Type: "String" }
              KeyColumnNames: ["id"]
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["prefix", "embedding"]
              DataColumnNames: ["covered"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 }, clusters: 4, levels: 5 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector"),
            { NLs::PathExist,
              NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
              NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
              NLs::IndexKeys({"prefix", "embedding"}),
              NLs::IndexDataColumns({"covered"}),
              NLs::KMeansTreeDescription(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                                          Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
                                          1024,
                                          4,
                                          5
                                          ),
            });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplPrefixTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PrefixTable, {"prefix", IdColumn}, {}, {"prefix", IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplLevelTable"),
            { NLs::PathExist,
              NLs::CheckColumns(LevelTable, {ParentColumn, IdColumn, CentroidColumn}, {}, {ParentColumn, IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplPostingTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PostingTable, {ParentColumn, "id", "covered"}, {}, {ParentColumn, "id"}, true) });


        TVector<ui64> dropTxIds;
        TestDropTable(runtime, dropTxIds.emplace_back(++txId), "/MyRoot", "vectors");
        env.TestWaitNotification(runtime, dropTxIds);
    }

    Y_UNIT_TEST(CreateTablePrefixInvalidKeyType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "embedding" Type: "String" }
              Columns { Name: "covered" Type: "String" }
              Columns { Name: "prefix" Type: "Float" }
              KeyColumnNames: ["id"]
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["prefix", "embedding"]
              DataColumnNames: ["covered"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 }, clusters: 4, levels: 5 } }
            }
        )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector"),
            { NLs::PathNotExist });
    }


    Y_UNIT_TEST(CreateTableCoveredEmbedding) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "embedding" Type: "String" }
              Columns { Name: "another" Type: "String" }
              KeyColumnNames: ["id"]
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["embedding"]
              DataColumnNames: ["embedding"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 }, clusters: 4, levels: 5 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector"),
            { NLs::PathExist,
              NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
              NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
              NLs::IndexKeys({"embedding"}),
              NLs::IndexDataColumns({"embedding"}),
              NLs::KMeansTreeDescription(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                                          Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
                                          1024,
                                          4,
                                          5
                                          ),
            });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplLevelTable"),
            { NLs::PathExist,
              NLs::CheckColumns(LevelTable, {ParentColumn, IdColumn, CentroidColumn}, {}, {ParentColumn, IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplPostingTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PostingTable, {ParentColumn, "id", "embedding"}, {}, {ParentColumn, "id"}, true) });
    }

    Y_UNIT_TEST(CreateTableMultiColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id1" Type: "String" }
              Columns { Name: "id2" Type: "String" }
              Columns { Name: "embedding" Type: "String" }
              Columns { Name: "covered1" Type: "String" }
              Columns { Name: "covered2" Type: "String" }
              Columns { Name: "another1" Type: "String" }
              Columns { Name: "another2" Type: "String" }
              KeyColumnNames: ["id1", "id2"]
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["embedding"]
              DataColumnNames: ["covered1", "covered2"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 }, clusters: 10, levels: 3 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector"),
            { NLs::PathExist,
              NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
              NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
              NLs::IndexKeys({"embedding"}),
              NLs::IndexDataColumns({"covered1", "covered2"}),
            });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplLevelTable"),
            { NLs::PathExist,
              NLs::CheckColumns(LevelTable, {ParentColumn, IdColumn, CentroidColumn}, {}, {ParentColumn, IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplPostingTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PostingTable, {ParentColumn, "id1", "id2", "covered1", "covered2"}, {}, {ParentColumn, "id1", "id2"}, true) });
    }

    Y_UNIT_TEST(VectorKmeansTreeImplTable) {
      // partition
      NKikimrSchemeOp::TPartitionConfig baseTablePartitionConfig;
      NKikimrSchemeOp::TTableDescription indexTableDesc;
      // columns
      NKikimrSchemeOp::TTableDescription baseTableDescr;
      {
        auto* embedding = baseTableDescr.AddColumns();
        *embedding->MutableName() = "embedding";
      }
      {
        auto* data1 = baseTableDescr.AddColumns();
        *data1->MutableName() = "data1";
      }
      {
        auto* data2 = baseTableDescr.AddColumns();
        *data2->MutableName() = "data2";
      }
      {
        auto* prefix = baseTableDescr.AddColumns();
        *prefix->MutableName() = "prefix";
      }

      {
        auto desc = CalcVectorKmeansTreeLevelImplTableDesc(baseTablePartitionConfig, indexTableDesc);
        std::string_view expected[] = {ParentColumn, IdColumn, CentroidColumn};
        for (size_t i = 0; auto& column : desc.GetColumns()) {
          UNIT_ASSERT_STRINGS_EQUAL(column.GetName(), expected[i]);
          ++i;
        }
      }
      {
        THashSet<TString> indexDataColumns = {"data2", "data1"};
        auto desc = NTableIndex::CalcVectorKmeansTreePostingImplTableDesc(baseTableDescr, baseTablePartitionConfig, indexDataColumns, indexTableDesc, "something");
        std::string_view expected[] = {NTableIndex::NKMeans::ParentColumn, "data1", "data2"};
        for (size_t i = 0; auto& column : desc.GetColumns()) {
          UNIT_ASSERT_STRINGS_EQUAL(column.GetName(), expected[i]);
          ++i;
        }
      }
      {
        NTableIndex::TTableColumns implTableColumns = {{"prefix"}, {}};
        auto desc = CalcVectorKmeansTreePrefixImplTableDesc({}, baseTableDescr, baseTablePartitionConfig, implTableColumns, indexTableDesc);
        std::string_view expected[] = {IdColumn};
        for (size_t i = 0; auto& column : desc.GetColumns()) {
          UNIT_ASSERT_STRINGS_EQUAL(column.GetName(), expected[i]);
          ++i;
        }
      }      
    }

    Y_UNIT_TEST(CreateTableWithError) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // base table column should not contains reserved name ParentColumn
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "%s" Type: "String" }
              KeyColumnNames: ["id"]
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["%s"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 } } }
            }
        )", ParentColumn, ParentColumn), {NKikimrScheme::StatusInvalidParameter});

        // pk should not be covered
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "embedding" Type: "String" }
              KeyColumnNames: ["id"]
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["embedding"]
              DataColumnNames: ["id"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 } } }
            }
        )", {NKikimrScheme::StatusInvalidParameter});
    }

    // Verify that DeriveIndexSchemaVersion returns Max(impl-table AlterVersions)
    // rather than merely mirroring the index's own AlterVersion.
    //
    // Setup: create a vector index (EIndexTypeGlobalVectorKmeansTree) which has
    // two impl tables — indexImplLevelTable and indexImplPostingTable.
    //
    // Step 1: both impl tables start at AlterVersion=1; parent SchemaVersion must be 1.
    // Step 2: alter ONLY indexImplPostingTable's PartitionConfig (bump its AlterVersion
    //         to 2 while indexImplLevelTable stays at 1).
    // Step 3: parent SchemaVersion must now be 2 (max(1,2)=2).
    //
    // On a hypothetical pre-Phase-2 codebase that just mirrors the index's own
    // AlterVersion, step 3 would still report 1 (the index AlterVersion did not
    // change), so this assertion discriminates the two behaviors.
    Y_UNIT_TEST(VectorIndexSchemaVersionDerivedFromMaxImplTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "embedding" Type: "String" }
              KeyColumnNames: ["id"]
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["embedding"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 4 }, clusters: 4, levels: 2 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Helper: read the SchemaVersion reported by the parent table for "idx_vector".
        // This is the value DeriveIndexSchemaVersion() fills into
        // TIndexDescription::SchemaVersion (field 6 of the proto).
        auto readParentIndexSchemaVersion = [&]() -> ui64 {
            auto desc = DescribePrivatePath(runtime, "/MyRoot/vectors");
            const auto& table = desc.GetPathDescription().GetTable();
            for (int i = 0, n = table.TableIndexesSize(); i < n; ++i) {
                if (table.GetTableIndexes(i).GetName() == "idx_vector") {
                    return table.GetTableIndexes(i).GetSchemaVersion();
                }
            }
            return 0;
        };

        // Helper: read the impl table's own TableSchemaVersion from its private describe.
        auto readImplTableSchemaVersion = [&](const TString& implPath) -> ui64 {
            auto desc = DescribePrivatePath(runtime, implPath);
            return desc.GetPathDescription().GetTable().GetTableSchemaVersion();
        };

        // Step 1: initial state — both impl tables at AlterVersion=1.
        const ui64 levelV0 = readImplTableSchemaVersion("/MyRoot/vectors/idx_vector/indexImplLevelTable");
        const ui64 postingV0 = readImplTableSchemaVersion("/MyRoot/vectors/idx_vector/indexImplPostingTable");
        UNIT_ASSERT_VALUES_EQUAL_C(levelV0, 1, "expected initial indexImplLevelTable.TableSchemaVersion=1");
        UNIT_ASSERT_VALUES_EQUAL_C(postingV0, 1, "expected initial indexImplPostingTable.TableSchemaVersion=1");

        const ui64 parentV0 = readParentIndexSchemaVersion();
        UNIT_ASSERT_VALUES_EQUAL_C(parentV0, 1, "expected initial parent.Indexes[idx_vector].SchemaVersion=1");

        // Step 2: alter ONLY indexImplPostingTable (bump its AlterVersion to 2).
        // Changing PartitioningPolicy on the default column family is the one
        // alter that schemeshard accepts on an index impl table (see ut_base.cpp
        // around line 11224).  The parent index's own AlterVersion is untouched.
        TestAlterTable(runtime, ++txId, "/MyRoot/vectors/idx_vector", R"(
            Name: "indexImplPostingTable"
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 1
                    SizeToSplit: 2000000000
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Verify only the posting table's version advanced.
        const ui64 levelV1 = readImplTableSchemaVersion("/MyRoot/vectors/idx_vector/indexImplLevelTable");
        const ui64 postingV1 = readImplTableSchemaVersion("/MyRoot/vectors/idx_vector/indexImplPostingTable");
        UNIT_ASSERT_VALUES_EQUAL_C(levelV1, 1, "indexImplLevelTable should still be at version 1 after altering only posting table");
        UNIT_ASSERT_VALUES_EQUAL_C(postingV1, 2, "indexImplPostingTable should be at version 2 after alter");

        // Step 3: parent SchemaVersion must now equal max(Level=1, Posting=2) = 2.
        // On a pre-Phase-2 mirror-only codebase this would still be 1, because
        // the index's own AlterVersion was never bumped.
        const ui64 parentV1 = readParentIndexSchemaVersion();
        UNIT_ASSERT_VALUES_EQUAL_C(parentV1, 2,
            "parent.Indexes[idx_vector].SchemaVersion must equal max(level.AV=1, posting.AV=2)=2 "
            "after altering only the posting impl table");
    }
}

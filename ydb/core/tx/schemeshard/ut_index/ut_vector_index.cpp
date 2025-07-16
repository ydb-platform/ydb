#include <ydb/core/base/path.h>
#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>


using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NTableIndex;
using namespace NKikimr::NTableIndex::NTableVectorKmeansTreeIndex;

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
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 } } }
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
        std::string_view expected[] = {NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn, "data1", "data2"};
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
}

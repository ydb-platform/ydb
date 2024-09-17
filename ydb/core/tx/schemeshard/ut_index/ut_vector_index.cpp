#include <ydb/core/base/path.h>
#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>


using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
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
              VectorIndexKmeansTreeDescription: { Settings : { distance: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector"),
            { NLs::PathExist,
              NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
              NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
              NLs::IndexKeys({"embedding"}),
              NLs::IndexDataColumns({"covered"}),
              NLs::VectorIndexDescription(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                                          Ydb::Table::VectorIndexSettings::SIMILARITY_UNSPECIFIED,
                                          Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
                                          1024
                                          ),
            });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplLevelTable"),
            { NLs::PathExist,
              NLs::CheckColumns(LevelTable, {LevelTable_ParentIdColumn, LevelTable_IdColumn, LevelTable_EmbeddingColumn}, {}, {LevelTable_ParentIdColumn, LevelTable_IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplPostingTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PostingTable, {PostingTable_ParentIdColumn, "id", "covered"}, {}, {PostingTable_ParentIdColumn, "id"}, true) });


        TVector<ui64> dropTxIds;
        TestDropTable(runtime, dropTxIds.emplace_back(++txId), "/MyRoot", "vectors");
        env.TestWaitNotification(runtime, dropTxIds);              
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
              VectorIndexKmeansTreeDescription: { Settings : { distance: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector"),
            { NLs::PathExist,
              NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree),
              NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
              NLs::IndexKeys({"embedding"}),
              NLs::IndexDataColumns({"embedding"}),
              NLs::VectorIndexDescription(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                                          Ydb::Table::VectorIndexSettings::SIMILARITY_UNSPECIFIED,
                                          Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
                                          1024
                                          ),
            });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplLevelTable"),
            { NLs::PathExist,
              NLs::CheckColumns(LevelTable, {LevelTable_ParentIdColumn, LevelTable_IdColumn, LevelTable_EmbeddingColumn}, {}, {LevelTable_ParentIdColumn, LevelTable_IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplPostingTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PostingTable, {PostingTable_ParentIdColumn, "id", "embedding"}, {}, {PostingTable_ParentIdColumn, "id"}, true) });
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
              VectorIndexKmeansTreeDescription: { Settings : { distance: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 } }
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
              NLs::CheckColumns(LevelTable, {LevelTable_ParentIdColumn, LevelTable_IdColumn, LevelTable_EmbeddingColumn}, {}, {LevelTable_ParentIdColumn, LevelTable_IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplPostingTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PostingTable, {PostingTable_ParentIdColumn, "id1", "id2", "covered1", "covered2"}, {}, {PostingTable_ParentIdColumn, "id1", "id2"}, true) });
    } 

    Y_UNIT_TEST(VectorKmeansTreePostingImplTable) {
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
      NTableIndex::TTableColumns implTableColumns = {{"data2", "data1"}, {}};
      auto desc = CalcVectorKmeansTreePostingImplTableDesc(baseTableDescr, baseTablePartitionConfig, implTableColumns, indexTableDesc, "something");
      std::string_view expected[] = {NTableIndex::NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn, "data1", "data2"};
      for (size_t i = 0; auto& column : desc.GetColumns()) {
        UNIT_ASSERT_STRINGS_EQUAL(column.GetName(), expected[i]);
        ++i;
      }
    }

    Y_UNIT_TEST(CreateTableWithError) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // base table column should not contains reserved name ParentIdColumn
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
              VectorIndexKmeansTreeDescription: { Settings : { distance: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 } }
            }
        )", NTableIndex::NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn, NTableIndex::NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn), {NKikimrScheme::StatusInvalidParameter});

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
              VectorIndexKmeansTreeDescription: { Settings : { distance: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 } }
            }
        )", {NKikimrScheme::StatusInvalidParameter});
    }     
}

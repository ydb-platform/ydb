#include <ydb/core/base/path.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

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
              Columns { Name: "otherColumn1" Type: "String" }
              Columns { Name: "otherColumn2" Type: "String" }
              Columns { Name: "otherColumn3" Type: "String" }
              KeyColumnNames: ["id"]
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["embedding"]
              Type: EIndexTypeGlobalVector
              VectorIndexDescription {
                IndexType: INDEX_TYPE_KMEANS_TREE,
                Distance: DISTANCE_COSINE,
                VectorType: VECTOR_TYPE_FLOAT
              }             
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector"),
            { NLs::PathExist,
              NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVector),
              NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
              NLs::IndexKeys({"embedding"}),
              NLs::VectorIndexDescription(Ydb::Table::GlobalVectorIndex::INDEX_TYPE_KMEANS_TREE, 
                                          Ydb::Table::GlobalVectorIndex::DISTANCE_COSINE,
                                          Ydb::Table::GlobalVectorIndex::SIMILARITY_UNSPECIFIED,
                                          Ydb::Table::GlobalVectorIndex::VECTOR_TYPE_FLOAT
                                          ),
            });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_vector/indexImplTable"),
            { NLs::PathExist,
              NLs::CheckColumns("indexImplTable", {"level", "id", "centroid", "ids"}, {}, {"level", "id"}) });
    } 
}

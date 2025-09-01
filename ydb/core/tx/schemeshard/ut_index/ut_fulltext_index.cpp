#include <ydb/core/base/path.h>
#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>


using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NTableIndex;
using namespace NKikimr::NTableIndex::NTableVectorKmeansTreeIndex;

Y_UNIT_TEST_SUITE(TFulltextIndexTests) {
    Y_UNIT_TEST(CreateTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "texts"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "text" Type: "String" }
              Columns { Name: "covered" Type: "String" }
              Columns { Name: "another" Type: "String" }
              KeyColumnNames: ["id"]
            }
            IndexDescription {
              Name: "idx_fulltext"
              KeyColumnNames: ["text"]
              DataColumnNames: ["covered"]
              Type: EIndexTypeGlobalFulltext
              FulltextIndexDescription: { Settings: { layout: FLAT, tokenizer: STANDARD, use_filter_ngram: true, filter_ngram_max_length: 42 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // NKikimrSchemeOp::TDescribeOptions opts;
        Cout << DescribePath(runtime, "/MyRoot/texts/idx_fulltext").DebugString() << Endl;

        for (ui32 reboot = 0; reboot < 2; reboot++) {
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/idx_fulltext"),{ 
                NLs::PathExist,
                NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalFulltext),
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                NLs::IndexKeys({"text"}),
                NLs::IndexDataColumns({"covered"}),
                NLs::SpecializedIndexDescription(""),
            });

            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }

        return;
        
        

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_fulltext/indexImplLevelTable"),
            { NLs::PathExist,
              NLs::CheckColumns(LevelTable, {ParentColumn, IdColumn, CentroidColumn}, {}, {ParentColumn, IdColumn}, true) });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/vectors/idx_fulltext/indexImplPostingTable"),
            { NLs::PathExist,
              NLs::CheckColumns(PostingTable, {ParentColumn, "id", "covered"}, {}, {ParentColumn, "id"}, true) });


        TVector<ui64> dropTxIds;
        TestDropTable(runtime, dropTxIds.emplace_back(++txId), "/MyRoot", "vectors");
        env.TestWaitNotification(runtime, dropTxIds);
    }
}

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;


Y_UNIT_TEST_SUITE(VectorIndexBuildTestReboots) {
    Y_UNIT_TEST_WITH_REBOOTS(BaseCase) {
        // Without killOnCommit, the schemeshard doesn't get rebooted on TEvDataShard::Ev***KMeansResponse's,
        // and thus the vector index build process is never interrupted at all because there are no other
        // events to reboot on.
        T t(true /*killOnCommit*/);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "dir/Table"
                    Columns { Name: "key"       Type: "Uint32" }
                    Columns { Name: "embedding" Type: "String" }
                    Columns { Name: "value"     Type: "String" }
                    KeyColumnNames: ["key"]
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 50 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 150 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 250 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 350 } } } }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/dir/Table", true, 0, 0, 50);
                WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/dir/Table", true, 1, 50, 150);
                WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/dir/Table", true, 2, 150, 250);
                WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/dir/Table", true, 3, 250, 350);
                WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/dir/Table", true, 4, 350, 400);
            }

            ui64 buildIndexId = ++t.TxId;
            auto sender = runtime.AllocateEdgeActor();
            auto request = CreateBuildIndexRequest(buildIndexId, "/MyRoot", "/MyRoot/dir/Table", TBuildIndexConfig{
                "index1", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, {"embedding"}, {"value"}
            });
            // with too many scan events, the test works infinite time
            request->Record.MutableSettings()->MutableScanSettings()->Clear();
            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, request);

            {
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL((ui64)descr.GetIndexBuild().GetState(), (ui64)Ydb::Table::IndexBuildState::STATE_PREPARING);
            }

            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL((ui64)descr.GetIndexBuild().GetState(), (ui64)Ydb::Table::IndexBuildState::STATE_DONE);
            }

            {
                TInactiveZone inactive(activeZone);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(1)});
                const TString indexPath = "/MyRoot/dir/Table/index1";
                TestDescribeResult(DescribePath(runtime, indexPath, true, true, true),
                                   {NLs::PathExist,
                                    NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});
                using namespace NTableIndex::NTableVectorKmeansTreeIndex;
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + LevelTable, true, true, true),
                                   {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + PostingTable, true, true, true),
                                   {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + PostingTable + BuildSuffix0, true, true, true),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + PostingTable + BuildSuffix1, true, true, true),
                                   {NLs::PathNotExist});

                // Check row count in the posting table
                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/dir/Table/index1/indexImplPostingTable");
                    Cerr << "... posting table contains " << rows << " rows" << Endl;
                    UNIT_ASSERT_VALUES_EQUAL(rows, 400);
                }

            }
        });
    }
}

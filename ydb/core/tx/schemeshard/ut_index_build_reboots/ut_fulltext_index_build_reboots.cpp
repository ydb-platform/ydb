#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;


Y_UNIT_TEST_SUITE(FulltextIndexBuildTestReboots) {
    // Without killOnCommit, the schemeshard doesn't get rebooted on TEvDataShard::BuildFulltext**Response's,
    // and thus the fulltext index build process is never interrupted at all because there are no other
    // events to reboot on.
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(BaseCase, 4 /*rebootBuckets*/, 4 /*pipeResetBuckets*/, true /*killOnCommit*/) {
        // speed up the test:
        // only check scheme shard reboots
        t.TabletIds.clear();
        t.TabletIds.push_back(t.SchemeShardTabletId);
        // white list some more events
        t.NoRebootEventTypes.insert(TEvSchemeShard::EvModifySchemeTransaction);
        t.NoRebootEventTypes.insert(TSchemeBoardEvents::EvUpdateAck);
        t.NoRebootEventTypes.insert(TEvSchemeShard::EvNotifyTxCompletionRegistered);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvServerDisconnected);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvServerConnected);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvClientConnected);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvClientDestroyed);
        t.NoRebootEventTypes.insert(TEvDataShard::EvBuildIndexProgressResponse);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "texts"
                    Columns { Name: "id" Type: "Uint64" }
                    Columns { Name: "text" Type: "String" }
                    Columns { Name: "data" Type: "String" }
                    KeyColumnNames: [ "id" ]
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 6 } } } }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TVector<TCell> cells = {
                    TCell::Make((ui64)1), TCell(TStringBuf("green apple")), TCell(TStringBuf("one")),
                    TCell::Make((ui64)2), TCell(TStringBuf("red apple and blue apple")), TCell(TStringBuf("two")),
                    TCell::Make((ui64)3), TCell(TStringBuf("yellow apple")), TCell(TStringBuf("three")),
                    TCell::Make((ui64)4), TCell(TStringBuf("red car")), TCell(TStringBuf("four")),
                    TCell::Make((ui64)5), TCell(TStringBuf("cat eats mice every day")), TCell(TStringBuf("five")),
                };
                WriteOp(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/texts",
                    0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                    {1, 2, 3}, TSerializedCellMatrix(cells, 5, 3), true);

                cells = {
                    TCell::Make((ui64)6), TCell(TStringBuf("dogs eat mice but sometimes mice flee")), TCell(TStringBuf("six")),
                    TCell::Make((ui64)7), TCell(TStringBuf("dog eats mice and cats")), TCell(TStringBuf("seven")),
                    TCell::Make((ui64)8), TCell(TStringBuf("dogs like bones and mice")), TCell(TStringBuf("eight")),
                    TCell::Make((ui64)9), TCell(TStringBuf("foxes eat mice too")), TCell(TStringBuf("nine")),
                    TCell::Make((ui64)10), TCell(TStringBuf("everyone eats mice")), TCell(TStringBuf("ten")),
                };
                WriteOp(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/texts",
                    1, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                    {1, 2, 3}, TSerializedCellMatrix(cells, 5, 3), true);
            }

            const ui64 buildIndexId = ++t.TxId;
            {
                auto indexColumns = TVector<TString>{"text"};
                auto sender = runtime.AllocateEdgeActor();
                auto request = CreateBuildIndexRequest(buildIndexId, "/MyRoot", "/MyRoot/texts", TBuildIndexConfig{
                    "index1", NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance, indexColumns, {"data"}, {}
                });
                auto& fulltext = *request->Record.MutableSettings()->mutable_index()->mutable_global_fulltext_relevance_index()->mutable_fulltext_settings();
                fulltext.set_layout(Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE);  // Layout is required for internal processing
                auto& analyzers = *fulltext.add_columns()->mutable_analyzers();
                fulltext.mutable_columns()->at(0).set_column("text");
                analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
                // with too many scan events, the test works infinite time
                request->Record.MutableSettings()->MutableScanSettings()->Clear();
                ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, request);
            }

            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                TInactiveZone inactive(activeZone);

                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL((ui64)descr.GetIndexBuild().GetState(), (ui64)Ydb::Table::IndexBuildState::STATE_DONE);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/texts"),
                    {NLs::PathExist, NLs::IndexesCount(1)});
                const TString indexPath = "/MyRoot/texts/index1";
                TestDescribeResult(DescribePath(runtime, indexPath, true, true, true),
                    {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + NTableIndex::ImplTable, true, true, true), {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + NTableIndex::NFulltext::DictTable, true, true, true), {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + NTableIndex::NFulltext::DocsTable, true, true, true), {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + NTableIndex::NFulltext::StatsTable, true, true, true), {NLs::PathExist});

                // Check row counts in index tables
                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/texts/index1/" + TString(NTableIndex::ImplTable));
                    Cerr << "... posting table contains " << rows << " rows" << Endl;
                    UNIT_ASSERT_VALUES_EQUAL(rows, 38);
                }

                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/texts/index1/" + TString(NTableIndex::NFulltext::DictTable));
                    Cerr << "... dictionary table contains " << rows << " rows" << Endl;
                    UNIT_ASSERT_VALUES_EQUAL(rows, 24);
                }

                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/texts/index1/" + TString(NTableIndex::NFulltext::DocsTable));
                    Cerr << "... document table contains " << rows << " rows" << Endl;
                    UNIT_ASSERT_VALUES_EQUAL(rows, 10);
                }

                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/texts/index1/" + TString(NTableIndex::NFulltext::StatsTable));
                    Cerr << "... statistics table contains " << rows << " rows" << Endl;
                    UNIT_ASSERT_VALUES_EQUAL(rows, 1);
                }

            }
        });
    }
}

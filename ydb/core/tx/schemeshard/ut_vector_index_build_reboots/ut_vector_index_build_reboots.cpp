#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;


static void WriteRows(TTestActorRuntime& runtime, ui64 tabletId, ui32 key, ui32 index) {
    TString writeQuery = Sprintf(R"(
        (
            (let keyNull   '( '('key   (Null) ) ) )
            (let row0   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )
            (let key0   '( '('key   (Uint32 '%u ) ) ) )
            (let row0   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )
            (let key1   '( '('key   (Uint32 '%u ) ) ) )
            (let row1   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )
            (let key2   '( '('key   (Uint32 '%u ) ) ) )
            (let row2   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )
            (let key3   '( '('key   (Uint32 '%u ) ) ) )
            (let row3   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )
            (let key4   '( '('key   (Uint32 '%u ) ) ) )
            (let row4   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )
            (let key5   '( '('key   (Uint32 '%u ) ) ) )
            (let row5   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )
            (let key6   '( '('key   (Uint32 '%u ) ) ) )
            (let row6   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )
            (let key7   '( '('key   (Uint32 '%u ) ) ) )
            (let row7   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )
            (let key8   '( '('key   (Uint32 '%u ) ) ) )
            (let row8   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )
            (let key9   '( '('key   (Uint32 '%u ) ) ) )
            (let row9   '( '('embedding (String '%s ) )  '('value (String 'aaaa) ) ) )

            (return (AsList
                        (UpdateRow '__user__Table keyNull row0)
                        (UpdateRow '__user__Table key0 row0)
                        (UpdateRow '__user__Table key1 row1)
                        (UpdateRow '__user__Table key2 row2)
                        (UpdateRow '__user__Table key3 row3)
                        (UpdateRow '__user__Table key4 row4)
                        (UpdateRow '__user__Table key5 row5)
                        (UpdateRow '__user__Table key6 row6)
                        (UpdateRow '__user__Table key7 row7)
                        (UpdateRow '__user__Table key8 row8)
                        (UpdateRow '__user__Table key9 row9)
                     )
            )
        )
    )",
                       std::to_string(1000*index + 0).c_str(),
         1000*key + 0, std::to_string(1000*index + 0).c_str(),
         1000*key + 1, std::to_string(1000*index + 1).c_str(),
         1000*key + 2, std::to_string(1000*index + 2).c_str(),
         1000*key + 3, std::to_string(1000*index + 3).c_str(),
         1000*key + 4, std::to_string(1000*index + 4).c_str(),
         1000*key + 5, std::to_string(1000*index + 5).c_str(),
         1000*key + 6, std::to_string(1000*index + 6).c_str(),
         1000*key + 7, std::to_string(1000*index + 7).c_str(),
         1000*key + 8, std::to_string(1000*index + 8).c_str(),
         1000*key + 9, std::to_string(1000*index + 9).c_str());

    NKikimrMiniKQL::TResult result;
    TString err;
    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
    UNIT_ASSERT_VALUES_EQUAL(err, "");
    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
}

Y_UNIT_TEST_SUITE(VectorIndexBuildTestReboots) {
    Y_UNIT_TEST_WITH_REBOOTS(BaseCase) {
        T t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                  Name: "dir/Table"
                  Columns { Name: "key"       Type: "Uint32" }
                  Columns { Name: "embedding" Type: "String" }
                  Columns { Name: "value"     Type: "String" }
                  KeyColumnNames: ["key"]
                  UniformPartitionsCount: 2
                 )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (ui32 delta = 0; delta < 2; ++delta) {
                    WriteRows(runtime, TTestTxConfig::FakeHiveTablets, 1 + delta, 100 + delta);
                }

                // Check src shard has the lead key as null
                {
                    NKikimrMiniKQL::TResult result;
                    TString err;
                    ui32 status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, R"(
                    (
                        (let range '('('key (Null) (Void))))
                        (let columns '('key 'embedding))
                        (let result (SelectRange '__user__Table range columns '()))
                        (return (AsList (SetResult 'Result result)))
                    )
                    )", result, err);

                    UNIT_ASSERT_VALUES_EQUAL_C(status, static_cast<ui32>(NKikimrProto::OK), err);
                    UNIT_ASSERT_VALUES_EQUAL(err, "");

                    //                                   V -- here the null key
                    NKqp::CompareYson(R"([[[[[["101000"];#];[["100000"];["1000"]];[["100001"];["1001"]];[["100002"];["1002"]];[["100003"];["1003"]];[["100004"];["1004"]];[["100005"];["1005"]];[["100006"];["1006"]];[["100007"];["1007"]];[["100008"];["1008"]];[["100009"];["1009"]];[["101000"];["2000"]];[["101001"];["2001"]];[["101002"];["2002"]];[["101003"];["2003"]];[["101004"];["2004"]];[["101005"];["2005"]];[["101006"];["2006"]];[["101007"];["2007"]];[["101008"];["2008"]];[["101009"];["2009"]]];%false]]])", result);
                }
            }

            AsyncBuildVectorIndex(runtime,  ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/dir/Table", "index1", "embedding", {"value"});
            ui64 buildIndexId = t.TxId;

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
            }
        });
    }
}

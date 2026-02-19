#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

static void WriteRows(TTestActorRuntime& runtime, ui64 tabletId, ui32 key, ui32 index) {
    TString writeQuery = Sprintf(R"(
        (
            (let keyNull   '( '('key   (Null) ) ) )
            (let row0   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key0   '( '('key   (Uint32 '%u ) ) ) )
            (let row0   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key1   '( '('key   (Uint32 '%u ) ) ) )
            (let row1   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key2   '( '('key   (Uint32 '%u ) ) ) )
            (let row2   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key3   '( '('key   (Uint32 '%u ) ) ) )
            (let row3   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key4   '( '('key   (Uint32 '%u ) ) ) )
            (let row4   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key5   '( '('key   (Uint32 '%u ) ) ) )
            (let row5   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key6   '( '('key   (Uint32 '%u ) ) ) )
            (let row6   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key7   '( '('key   (Uint32 '%u ) ) ) )
            (let row7   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key8   '( '('key   (Uint32 '%u ) ) ) )
            (let row8   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
            (let key9   '( '('key   (Uint32 '%u ) ) ) )
            (let row9   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )

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
         1000*index + 0,
         1000*key + 0, 1000*index + 0,
         1000*key + 1, 1000*index + 1,
         1000*key + 2, 1000*index + 2,
         1000*key + 3, 1000*index + 3,
         1000*key + 4, 1000*index + 4,
         1000*key + 5, 1000*index + 5,
         1000*key + 6, 1000*index + 6,
         1000*key + 7, 1000*index + 7,
         1000*key + 8, 1000*index + 8,
         1000*key + 9, 1000*index + 9);

    NKikimrMiniKQL::TResult result;
    TString err;
    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
    UNIT_ASSERT_VALUES_EQUAL(err, "");
    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
}

static void WriteNullKey(TTestActorRuntime& runtime, ui64 tabletId, ui32 index) {
    TString writeQuery = Sprintf(R"(
        (
            (let keyNull '( '('key   (Null) ) ) )
            (let row     '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )

            (return (AsList
                        (UpdateRow '__user__Table keyNull row)
                     )
            )
        )
    )",
    index);

    NKikimrMiniKQL::TResult result;
    TString err;
    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
    UNIT_ASSERT_VALUES_EQUAL(err, "");
    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
}

Y_UNIT_TEST_SUITE(IndexBuildTestReboots) {

    void DoBaseCase(TTestWithReboots& t, NKikimrSchemeOp::EIndexType indexType) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableAddUniqueIndex(true);
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                  Name: "dir/Table"
                  Columns { Name: "key"     Type: "Uint32" }
                  Columns { Name: "index"   Type: "Uint32" }
                  Columns { Name: "value"   Type: "Utf8"   }
                  KeyColumnNames: ["key"]
                  UniformPartitionsCount: 2
                 )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (ui32 delta = 0; delta < 2; ++delta) {
                    WriteRows(runtime, TTestTxConfig::FakeHiveTablets, 1 + delta, 100 + delta);
                }

                if (indexType == NKikimrSchemeOp::EIndexTypeGlobalUnique) {
                    // Unique index key with unique null table key
                    WriteNullKey(runtime, TTestTxConfig::FakeHiveTablets, 100500);
                }

                // Check src shard has the lead key as null
                {
                    NKikimrMiniKQL::TResult result;
                    TString err;
                    ui32 status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, R"(
                    (
                        (let range '('('key (Null) (Void))))
                        (let columns '('key 'index))
                        (let result (SelectRange '__user__Table range columns '()))
                        (return (AsList (SetResult 'Result result)))
                    )
                    )", result, err);

                    UNIT_ASSERT_VALUES_EQUAL_C(status, static_cast<ui32>(NKikimrProto::OK), err);
                    UNIT_ASSERT_VALUES_EQUAL(err, "");

                    if (indexType == NKikimrSchemeOp::EIndexTypeGlobalUnique) {
                        //                                   V -- here the null key
                        NKqp::CompareYson(R"([[[[[["100500"];#];[["100000"];["1000"]];[["100001"];["1001"]];[["100002"];["1002"]];[["100003"];["1003"]];[["100004"];["1004"]];[["100005"];["1005"]];[["100006"];["1006"]];[["100007"];["1007"]];[["100008"];["1008"]];[["100009"];["1009"]];[["101000"];["2000"]];[["101001"];["2001"]];[["101002"];["2002"]];[["101003"];["2003"]];[["101004"];["2004"]];[["101005"];["2005"]];[["101006"];["2006"]];[["101007"];["2007"]];[["101008"];["2008"]];[["101009"];["2009"]]];%false]]])", result);
                    } else {
                        //                                   V -- here the null key
                        NKqp::CompareYson(R"([[[[[["101000"];#];[["100000"];["1000"]];[["100001"];["1001"]];[["100002"];["1002"]];[["100003"];["1003"]];[["100004"];["1004"]];[["100005"];["1005"]];[["100006"];["1006"]];[["100007"];["1007"]];[["100008"];["1008"]];[["100009"];["1009"]];[["101000"];["2000"]];[["101001"];["2001"]];[["101002"];["2002"]];[["101003"];["2003"]];[["101004"];["2004"]];[["101005"];["2005"]];[["101006"];["2006"]];[["101007"];["2007"]];[["101008"];["2008"]];[["101009"];["2009"]]];%false]]])", result);
                    }
                }
            }

            AsyncBuildIndex(runtime,  ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/dir/Table", TBuildIndexConfig{"index1", indexType, {"index"}, {}, {}});
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

            TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table"),
                               {NLs::PathExist,
                                NLs::IndexesCount(1)});

            TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table/index1", true, true, true),
                               {NLs::PathExist,
                                NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});

            TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table/index1/indexImplTable", true, true, true),
                               {NLs::PathExist});

            // Check result
            {
                TInactiveZone inactive(activeZone);

                NKikimrMiniKQL::TResult result;
                TString err;
                ui32 status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets+2, R"(
                (
                    (let range '( '('index (Null) (Void))  '('key (Null) (Void))))
                    (let columns '('key 'index) )
                    (let result (SelectRange '__user__indexImplTable range columns '()))
                    (return (AsList (SetResult 'Result result) ))
                )
                )", result, err);

                UNIT_ASSERT_VALUES_EQUAL_C(status, static_cast<ui32>(NKikimrProto::OK), err);
                UNIT_ASSERT_VALUES_EQUAL(err, "");

                if (indexType == NKikimrSchemeOp::EIndexTypeGlobalUnique) {
                    // record with null is there ->                                                                                                                                                                                                                                  V -- here is the null
                    NKqp::CompareYson(R"([[[[[["100000"];["1000"]];[["100001"];["1001"]];[["100002"];["1002"]];[["100003"];["1003"]];[["100004"];["1004"]];[["100005"];["1005"]];[["100006"];["1006"]];[["100007"];["1007"]];[["100008"];["1008"]];[["100009"];["1009"]];[["100500"];#];[["101000"];["2000"]];[["101001"];["2001"]];[["101002"];["2002"]];[["101003"];["2003"]];[["101004"];["2004"]];[["101005"];["2005"]];[["101006"];["2006"]];[["101007"];["2007"]];[["101008"];["2008"]];[["101009"];["2009"]]];%false]]])", result);
                } else {
                    // record with null is there ->                                                                                                                                                                                                                                  V -- here is the null
                    NKqp::CompareYson(R"([[[[[["100000"];["1000"]];[["100001"];["1001"]];[["100002"];["1002"]];[["100003"];["1003"]];[["100004"];["1004"]];[["100005"];["1005"]];[["100006"];["1006"]];[["100007"];["1007"]];[["100008"];["1008"]];[["100009"];["1009"]];[["101000"];#];[["101000"];["2000"]];[["101001"];["2001"]];[["101002"];["2002"]];[["101003"];["2003"]];[["101004"];["2004"]];[["101005"];["2005"]];[["101006"];["2006"]];[["101007"];["2007"]];[["101008"];["2008"]];[["101009"];["2009"]]];%false]]])", result);
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(BaseCase, 2 /*rebootBuckets*/, 2 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DoBaseCase(t, NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(BaseCaseUniq, 4 /*rebootBuckets*/, 4 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DoBaseCase(t, NKikimrSchemeOp::EIndexTypeGlobalUnique);
    }

    void DoBaseCaseWithDataColumns(TTestWithReboots& t, NKikimrSchemeOp::EIndexType indexType) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableAddUniqueIndex(true);
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                  Name: "dir/Table"
                  Columns { Name: "key"     Type: "Uint32" }
                  Columns { Name: "index"   Type: "Uint32" }
                  Columns { Name: "value"   Type: "Utf8"   }
                  KeyColumnNames: ["key"]
                  UniformPartitionsCount: 2
                 )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (ui32 delta = 0; delta < 2; ++delta) {
                    WriteRows(runtime, TTestTxConfig::FakeHiveTablets, 1 + delta, 100 + delta);
                }

                if (indexType == NKikimrSchemeOp::EIndexTypeGlobalUnique) {
                    // Unique index key with unique null table key
                    WriteNullKey(runtime, TTestTxConfig::FakeHiveTablets, 100500);
                }
            }

            AsyncBuildIndex(runtime,  ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/dir/Table", TBuildIndexConfig{"index1", indexType, {"index"}, {"value"}, {}});
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

            TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table"),
                               {NLs::PathExist,
                                NLs::IndexesCount(1)});

            TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table/index1", true, true, true),
                               {NLs::PathExist,
                                NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});

            TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table/index1/indexImplTable", true, true, true),
                               {NLs::PathExist});

            // Check result
            {
                TInactiveZone inactive(activeZone);

                NKikimrMiniKQL::TResult result;
                TString err;
                ui32 status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets+2, R"(
                (
                    (let range '( '('index (Uint32 '0) (Void) )  '('key (Uint32 '0) (Void) )))
                    (let columns '('key 'index 'value) )
                    (let result (SelectRange '__user__indexImplTable range columns '()))
                    (return (AsList (SetResult 'Result result) ))
                )
                )", result, err);

                UNIT_ASSERT_VALUES_EQUAL_C(status, static_cast<ui32>(NKikimrProto::OK), err);
                UNIT_ASSERT_VALUES_EQUAL(err, "");

                if (indexType == NKikimrSchemeOp::EIndexTypeGlobalUnique) {
                    NKqp::CompareYson(R"([[[[[["100000"];["1000"];["aaaa"]];[["100001"];["1001"];["aaaa"]];[["100002"];["1002"];["aaaa"]];[["100003"];["1003"];["aaaa"]];[["100004"];["1004"];["aaaa"]];[["100005"];["1005"];["aaaa"]];[["100006"];["1006"];["aaaa"]];[["100007"];["1007"];["aaaa"]];[["100008"];["1008"];["aaaa"]];[["100009"];["1009"];["aaaa"]];[["100500"];#;["aaaa"]];[["101000"];["2000"];["aaaa"]];[["101001"];["2001"];["aaaa"]];[["101002"];["2002"];["aaaa"]];[["101003"];["2003"];["aaaa"]];[["101004"];["2004"];["aaaa"]];[["101005"];["2005"];["aaaa"]];[["101006"];["2006"];["aaaa"]];[["101007"];["2007"];["aaaa"]];[["101008"];["2008"];["aaaa"]];[["101009"];["2009"];["aaaa"]]];%false]]])", result);
                } else {
                    NKqp::CompareYson(R"([[[[[["100000"];["1000"];["aaaa"]];[["100001"];["1001"];["aaaa"]];[["100002"];["1002"];["aaaa"]];[["100003"];["1003"];["aaaa"]];[["100004"];["1004"];["aaaa"]];[["100005"];["1005"];["aaaa"]];[["100006"];["1006"];["aaaa"]];[["100007"];["1007"];["aaaa"]];[["100008"];["1008"];["aaaa"]];[["100009"];["1009"];["aaaa"]];[["101000"];#;["aaaa"]];[["101000"];["2000"];["aaaa"]];[["101001"];["2001"];["aaaa"]];[["101002"];["2002"];["aaaa"]];[["101003"];["2003"];["aaaa"]];[["101004"];["2004"];["aaaa"]];[["101005"];["2005"];["aaaa"]];[["101006"];["2006"];["aaaa"]];[["101007"];["2007"];["aaaa"]];[["101008"];["2008"];["aaaa"]];[["101009"];["2009"];["aaaa"]]];%false]]])", result);
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(BaseCaseWithDataColumns, 2 /*rebootBuckets*/, 2 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DoBaseCaseWithDataColumns(t, NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(BaseCaseWithDataColumnsUniq, 2 /*rebootBuckets*/, 2 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DoBaseCaseWithDataColumns(t, NKikimrSchemeOp::EIndexTypeGlobalUnique);
    }

    void DoDropIndex(TTestWithReboots& t, NKikimrSchemeOp::EIndexType indexType) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableAddUniqueIndex(true);
            {
                TInactiveZone inactive(activeZone);

                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                    TableDescription {
                      Name: "Table"
                      Columns { Name: "key"   Type: "Uint64" }
                      Columns { Name: "value0" Type: "Utf8" }
                      Columns { Name: "value1" Type: "Utf8" }
                      KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                      Name: "UserDefinedIndexByValue0"
                      KeyColumnNames: ["value0"]
                      Type: %s
                    }
                )", NKikimrSchemeOp::EIndexType_Name(indexType).c_str()));

                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::Finished,
                                    NLs::PathVersionEqual(3),
                                    NLs::IndexesCount(1)});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0"),
                                   {NLs::Finished,
                                    NLs::IndexType(indexType),
                                    NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                                    NLs::IndexKeys({"value0"})});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0/indexImplTable"),
                                   {NLs::Finished,
                                    NLs::PathVersionEqual(3)});
            }

        TestDropTableIndex(runtime, ++t.TxId, "/MyRoot", R"(
            TableName: "Table"
            IndexName: "UserDefinedIndexByValue0"
        )");
        t.TestEnv->TestWaitNotification(runtime, t.TxId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5),
                            NLs::IndexesCount(0)});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0/indexImplTable"),
                           {NLs::PathNotExist});
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(DropIndex) {
        DoDropIndex(t, NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST_WITH_REBOOTS(DropIndexUniq) {
        DoDropIndex(t, NKikimrSchemeOp::EIndexTypeGlobalUnique);
    }

    void DoDropIndexWithDataColumns(TTestWithReboots& t, NKikimrSchemeOp::EIndexType indexType) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableAddUniqueIndex(true);
            {
                TInactiveZone inactive(activeZone);

                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                    TableDescription {
                      Name: "Table"
                      Columns { Name: "key"   Type: "Uint64" }
                      Columns { Name: "value0" Type: "Utf8" }
                      Columns { Name: "value1" Type: "Utf8" }
                      KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                      Name: "UserDefinedIndexByValue0"
                      KeyColumnNames: ["value0"]
                      DataColumnNames: ["value1"]
                      Type: %s
                    }
                )", NKikimrSchemeOp::EIndexType_Name(indexType).c_str()));

                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::Finished,
                                    NLs::PathVersionEqual(3),
                                    NLs::IndexesCount(1)});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0"),
                                   {NLs::Finished,
                                    NLs::IndexType(indexType),
                                    NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                                    NLs::IndexKeys({"value0"})});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0/indexImplTable"),
                                   {NLs::Finished,
                                    NLs::PathVersionEqual(3)});
            }

        TestDropTableIndex(runtime, ++t.TxId, "/MyRoot", R"(
            TableName: "Table"
            IndexName: "UserDefinedIndexByValue0"
        )");
        t.TestEnv->TestWaitNotification(runtime, t.TxId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5),
                            NLs::IndexesCount(0)});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0/indexImplTable"),
                           {NLs::PathNotExist});
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropIndexWithDataColumns, 2 /*rebootBuckets*/, 2 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DoDropIndexWithDataColumns(t, NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST_WITH_REBOOTS(DropIndexWithDataColumnsUniq) {
        DoDropIndexWithDataColumns(t, NKikimrSchemeOp::EIndexTypeGlobalUnique);
    }

    void DoCancelBuild(TTestWithReboots& t, NKikimrSchemeOp::EIndexType indexType) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableAddUniqueIndex(true);
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                  Name: "dir/Table"
                  Columns { Name: "key"     Type: "Uint32" }
                  Columns { Name: "index"   Type: "Uint32" }
                  Columns { Name: "value"   Type: "Utf8"   }
                  KeyColumnNames: ["key"]
                  UniformPartitionsCount: 2
                 )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index) {
                    TString writeQuery = Sprintf(R"(
                        (
                            (let key0   '( '('key   (Uint32 '%u ) ) ) )
                            (let row0   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
                            (let key1   '( '('key   (Uint32 '%u ) ) ) )
                            (let row1   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
                            (let key2   '( '('key   (Uint32 '%u ) ) ) )
                            (let row2   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
                            (let key3   '( '('key   (Uint32 '%u ) ) ) )
                            (let row3   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
                            (let key4   '( '('key   (Uint32 '%u ) ) ) )
                            (let row4   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
                            (let key5   '( '('key   (Uint32 '%u ) ) ) )
                            (let row5   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
                            (let key6   '( '('key   (Uint32 '%u ) ) ) )
                            (let row6   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
                            (let key7   '( '('key   (Uint32 '%u ) ) ) )
                            (let row7   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
                            (let key8   '( '('key   (Uint32 '%u ) ) ) )
                            (let row8   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )
                            (let key9   '( '('key   (Uint32 '%u ) ) ) )
                            (let row9   '( '('index (Uint32 '%u ) )  '('value (Utf8 'aaaa) ) ) )

                            (return (AsList
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
                                                 1000*key + 0, 1000*index + 0,
                                                 1000*key + 1, 1000*index + 1,
                                                 1000*key + 2, 1000*index + 2,
                                                 1000*key + 3, 1000*index + 3,
                                                 1000*key + 4, 1000*index + 4,
                                                 1000*key + 5, 1000*index + 5,
                                                 1000*key + 6, 1000*index + 6,
                                                 1000*key + 7, 1000*index + 7,
                                                 1000*key + 8, 1000*index + 8,
                                                 1000*key + 9, 1000*index + 9);

                    NKikimrMiniKQL::TResult result;
                    TString err;
                    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                    UNIT_ASSERT_VALUES_EQUAL(err, "");
                    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
                };
                for (ui32 delta = 0; delta < 1; ++delta) {
                    fnWriteRow(TTestTxConfig::FakeHiveTablets, 1 + delta, 100 + delta);
                }

                TestBuildIndex(runtime,  ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/dir/Table", TBuildIndexConfig{"index1", indexType, {"index"}, {}, {}});
            }

            ui64 buildId = t.TxId;

            auto response = TestCancelBuildIndex(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", buildId,
                                                 TVector<Ydb::StatusIds::StatusCode>{Ydb::StatusIds::SUCCESS, Ydb::StatusIds::PRECONDITION_FAILED});

            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitNotification(runtime, buildId);

            auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);

            if (response.GetStatus() == Ydb::StatusIds::SUCCESS) {
                Y_ASSERT(descr.GetIndexBuild().GetState() == Ydb::Table::IndexBuildState::STATE_CANCELLED);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(6)});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table/index1", true, true, true),
                                   {NLs::PathNotExist});
            } else {
                Y_ASSERT(descr.GetIndexBuild().GetState() == Ydb::Table::IndexBuildState::STATE_DONE);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(1),
                                    NLs::PathVersionEqual(6)});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table/index1", true, true, true),
                                   {NLs::PathExist});
            }

            TestForgetBuildIndex(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", buildId);

        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(CancelBuild) {
        DoCancelBuild(t, NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST_WITH_REBOOTS(CancelBuildUniq) {
        DoCancelBuild(t, NKikimrSchemeOp::EIndexTypeGlobalUnique);
    }

    void DoIndexPartitioning(TTestWithReboots& t, NKikimrSchemeOp::EIndexType indexType) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableAddUniqueIndex(true);
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: [ "key" ]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            Ydb::Table::GlobalIndexSettings settings;
            UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
                partition_at_keys {
                    split_points {
                        type { tuple_type { elements { optional_type { item { type_id: UTF8 } } } } }
                        value { items { text_value: "alice" } }
                    }
                    split_points {
                        type { tuple_type { elements { optional_type { item { type_id: UTF8 } } } } }
                        value { items { text_value: "bob" } }
                    }
                }
                partitioning_settings {
                    min_partitions_count: 3
                    max_partitions_count: 3
                }
            )", &settings));

            const ui64 buildIndexId = ++t.TxId;
            AsyncBuildIndex(runtime, buildIndexId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{
                "Index", indexType, { "value" }, {},
                { NYdb::NTable::TGlobalIndexSettings::FromProto(settings) }
            });

            {
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_PREPARING);
            }

            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
            }

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                NLs::IsTable,
                NLs::IndexesCount(1)
            });

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Index"), {
                NLs::PathExist,
                NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)
            });

            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Index/indexImplTable", true, true), {
                NLs::IsTable,
                NLs::PartitionCount(3),
                NLs::MinPartitionsCountEqual(3),
                NLs::MaxPartitionsCountEqual(3),
                NLs::PartitionKeys({"alice", "bob", ""})
            });
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(IndexPartitioning, 2 /*rebootBuckets*/, 2 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DoIndexPartitioning(t, NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(IndexPartitioningUniq, 4 /*rebootBuckets*/, 4 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DoIndexPartitioning(t, NKikimrSchemeOp::EIndexTypeGlobalUnique);
    }

    void DoUniqueIndexValidationFails(TTestWithReboots& t, bool crossShardsViolation) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableAddUniqueIndex(true);
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "index_key_first_part" Type: "Uint32" }
                    Columns { Name: "index_key_second_part" Type: "Utf8" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: [ "key" ]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Fill nonunique index data
                TString writeQuery = R"(
                    (
                        (let key0 '( '('key (Uint32 '40 ) ) ) )
                        (let row0 '(
                                    '('index_key_first_part  (Uint32   '42     ) )
                                    '('index_key_second_part (Utf8     'alpha  ) )
                                    '('value                 (Utf8     'ydb    ) )
                                   ))

                        (let key1 '( '('key (Uint32 '45 ) ) ) )
                        (let row1 '(
                                    '('index_key_first_part  (Uint32   '42     ) )
                                    '('index_key_second_part (Utf8     'alpha  ) )
                                    '('value                 (Utf8     'YDB    ) )
                                   ))

                        (let key2 '( '('key (Uint32 '10000000 ) ) ) )
                        (let row2 '(
                                    '('index_key_first_part  (Uint32   '42     ) )
                                    '('index_key_second_part (Utf8     'alef   ) )
                                    '('value                 (Utf8     'ydb   ) )
                                   ))

                        (return (AsList
                                    (UpdateRow '__user__Table key0 row0)
                                    (UpdateRow '__user__Table key1 row1)
                                    (UpdateRow '__user__Table key2 row2)
                                )
                        )
                    )
                )";
                NKikimrMiniKQL::TResult result;
                TString err;
                NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, writeQuery, result, err);
                UNIT_ASSERT_VALUES_EQUAL(err, "");
                UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
            }

            // Set index shard exact borders
            const TString indexType = R"(
                type {
                    tuple_type {
                        # index_key_first_part
                        elements {
                            optional_type { item { type_id: UINT32 } }
                        }
                        # index_key_second_part
                        elements {
                            optional_type { item { type_id: UTF8 } }
                        }
                        # key
                        elements {
                            optional_type { item { type_id: UINT32 } }
                        }
                    }
                }
            )";
            const TString settingsProto = Sprintf(R"(
                partition_at_keys {
                    split_points {
                        %s
                        value {
                            # index_key_first_part
                            items {
                                uint32_value: 42
                            }
                            # index_key_second_part
                            items {
                                text_value: "alpha"
                            }
                            # key
                            items {
                                uint32_value: %u
                            }
                        }
                    }
                    split_points {
                        %s
                        value {
                            # index_key_first_part
                            items {
                                uint32_value: 42
                            }
                            # index_key_second_part
                            items {
                                text_value: "alpha"
                            }
                            # key
                            items {
                                uint32_value: 100500
                            }
                        }
                    }
                }
                partitioning_settings {
                    min_partitions_count: 3
                    max_partitions_count: 3
                }
            )", indexType.c_str(), crossShardsViolation ? 42 : 50, indexType.c_str());
            Ydb::Table::GlobalIndexSettings settings;
            UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(settingsProto, &settings));

            const ui64 buildIndexId = ++t.TxId;
            AsyncBuildIndex(runtime, buildIndexId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{
                "Index", NKikimrSchemeOp::EIndexTypeGlobalUnique, { "index_key_first_part", "index_key_second_part" }, {},
                { NYdb::NTable::TGlobalIndexSettings::FromProto(settings) }
            });

            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED);
            }

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                NLs::IsTable,
                NLs::IndexesCount(0)
            });
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(UniqueIndexValidationFailsInsideShard) {
        DoUniqueIndexValidationFails(t, false);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(UniqueIndexValidationFailsBetweenShards, 2 /*rebootBuckets*/, 2 /*pipeResetBuckets*/, false /*killOnCommit*/) {
        DoUniqueIndexValidationFails(t, true);
    }
}

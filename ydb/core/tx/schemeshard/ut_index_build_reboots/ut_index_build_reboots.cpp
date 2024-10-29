#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

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

Y_UNIT_TEST_SUITE(IndexBuildTestReboots) {

    Y_UNIT_TEST(BaseCase) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
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

                    //                                   V -- here the null key
                    NKqp::CompareYson(R"([[[[[["101000"];#];[["100000"];["1000"]];[["100001"];["1001"]];[["100002"];["1002"]];[["100003"];["1003"]];[["100004"];["1004"]];[["100005"];["1005"]];[["100006"];["1006"]];[["100007"];["1007"]];[["100008"];["1008"]];[["100009"];["1009"]];[["101000"];["2000"]];[["101001"];["2001"]];[["101002"];["2002"]];[["101003"];["2003"]];[["101004"];["2004"]];[["101005"];["2005"]];[["101006"];["2006"]];[["101007"];["2007"]];[["101008"];["2008"]];[["101009"];["2009"]]];%false]]])", result);
                }
            }

            AsyncBuildIndex(runtime,  ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/dir/Table", "index1", {"index"});
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

                // record with null is there ->                                                                                                                                                                                                                                  V -- here is the null
                NKqp::CompareYson(R"([[[[[["100000"];["1000"]];[["100001"];["1001"]];[["100002"];["1002"]];[["100003"];["1003"]];[["100004"];["1004"]];[["100005"];["1005"]];[["100006"];["1006"]];[["100007"];["1007"]];[["100008"];["1008"]];[["100009"];["1009"]];[["101000"];#];[["101000"];["2000"]];[["101001"];["2001"]];[["101002"];["2002"]];[["101003"];["2003"]];[["101004"];["2004"]];[["101005"];["2005"]];[["101006"];["2006"]];[["101007"];["2007"]];[["101008"];["2008"]];[["101009"];["2009"]]];%false]]])", result);
            }
        });
    }

    Y_UNIT_TEST(BaseCaseWithDataColumns) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
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
            }

            AsyncBuildIndex(runtime,  ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/dir/Table", "index1", {"index"}, {"value"});
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

                NKqp::CompareYson(R"([[[[[["100000"];["1000"];["aaaa"]];[["100001"];["1001"];["aaaa"]];[["100002"];["1002"];["aaaa"]];[["100003"];["1003"];["aaaa"]];[["100004"];["1004"];["aaaa"]];[["100005"];["1005"];["aaaa"]];[["100006"];["1006"];["aaaa"]];[["100007"];["1007"];["aaaa"]];[["100008"];["1008"];["aaaa"]];[["100009"];["1009"];["aaaa"]];[["101000"];#;["aaaa"]];[["101000"];["2000"];["aaaa"]];[["101001"];["2001"];["aaaa"]];[["101002"];["2002"];["aaaa"]];[["101003"];["2003"];["aaaa"]];[["101004"];["2004"];["aaaa"]];[["101005"];["2005"];["aaaa"]];[["101006"];["2006"];["aaaa"]];[["101007"];["2007"];["aaaa"]];[["101008"];["2008"];["aaaa"]];[["101009"];["2009"];["aaaa"]]];%false]]])", result);
            }
        });
    }

    Y_UNIT_TEST(DropIndex) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
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
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::Finished,
                                    NLs::PathVersionEqual(3),
                                    NLs::IndexesCount(1)});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0"),
                                   {NLs::Finished,
                                    NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
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

    Y_UNIT_TEST(DropIndexWithDataColumns) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
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
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::Finished,
                                    NLs::PathVersionEqual(3),
                                    NLs::IndexesCount(1)});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndexByValue0"),
                                   {NLs::Finished,
                                    NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
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

    Y_UNIT_TEST(CancelBuild) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
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
                    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
                };
                for (ui32 delta = 0; delta < 1; ++delta) {
                    fnWriteRow(TTestTxConfig::FakeHiveTablets, 1 + delta, 100 + delta);
                }

                TestBuildIndex(runtime,  ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/dir/Table", "index1", {"index"});
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

    Y_UNIT_TEST(IndexPartitioning) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
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
                "Index", NKikimrSchemeOp::EIndexTypeGlobal, { "value" }, {},
                { NYdb::NTable::TGlobalIndexSettings::FromProto(settings) }
            });

            {
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL((int)descr.GetIndexBuild().GetState(), (int)Ydb::Table::IndexBuildState::STATE_PREPARING);
            }

            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL((int)descr.GetIndexBuild().GetState(), (int)Ydb::Table::IndexBuildState::STATE_DONE);
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
}

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(IntermediateDirsReboots) {
    Y_UNIT_TEST(Fake) {
    }
    Y_UNIT_TEST(CreateDirWithIntermediateDirs) {
        const TString validScheme = "Valid/x/y/z";
        const TString invalidScheme = "Invalid/wr0ng n@me";
        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusSchemeError;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestMkDir(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateTableWithIntermediateDirs) {
        const TString validScheme = R"(
            Name: "Valid/x/y/z"
            Columns { Name: "RowId" Type: "Uint64" }
            KeyColumnNames: ["RowId"]
        )";
        const TString invalidScheme = R"(
            Name: "Invalid/wr0ng n@me"
            Columns { Name: "RowId" Type: "Uint64" }
            KeyColumnNames: ["WrongRowId"]
        )";
        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusSchemeError;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestCreateTable(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateTableWithIntermediateDirsAndRejectInSolomon) {
        const TString validScheme = R"(
            Name: "Valid/x/y/z"
            PartitionCount: 2
        )";
        const TString invalidScheme = R"(
            Name: "Invalid/a/b/c"
            PartitionCount: 2
            ChannelProfileId: 30
        )";

        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusInvalidParameter;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestCreateSolomon(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateTableWithIntermediateDirsAndRejectInTable) {
        const TString validScheme = R"(
            Name: "Valid/x/y/z/table_name"
            Columns { Name: "RowId" Type: "Uint64" }
            KeyColumnNames: ["RowId"]
        )";
        const TString invalidScheme = R"(
            Name: "Invalid/a/b/c/table_name"
            Columns { Name: "RowId" Type: "Uint64" }
            KeyColumnNames: ["RowId_Invalid"]
        )";
        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusSchemeError;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestCreateTable(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateKesusWithIntermediateDirs) {
        const TString validScheme = R"(
            Name: "Valid/x/y/z"
        )";
        const TString invalidScheme = R"(
            Name: "Invalid/wr0ng n@me"
        )";
        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusSchemeError;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestCreateKesus(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateSolomonWithIntermediateDirs) {
        const TString validScheme = R"(
            Name: "Valid/x/y/z"
            PartitionCount: 2
        )";
        const TString invalidScheme = R"(
            Name: "Invalid/wr0ng n@me"
        )";
        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusSchemeError;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestCreateSolomon(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateDirWithIntermediateDirsForceDrop) {
        CreateWithIntermediateDirsForceDrop([](TTestActorRuntime& runtime, ui64 txId, const TString& root) {
            AsyncMkDir(runtime, txId, root, "x/y/z");
        });
    }

    Y_UNIT_TEST(CreateTableWithIntermediateDirsForceDrop) {
        CreateWithIntermediateDirsForceDrop([](TTestActorRuntime& runtime, ui64 txId, const TString& root) {
            AsyncCreateTable(runtime, txId, root, R"(
                Name: "x/y/z"
                Columns { Name: "RowId" Type: "Uint64" }
                KeyColumnNames: ["RowId"]
            )");
        });
    }

    Y_UNIT_TEST(CreateKesusWithIntermediateDirsForceDrop) {
        CreateWithIntermediateDirsForceDrop([](TTestActorRuntime& runtime, ui64 txId, const TString& root) {
            AsyncCreateKesus(runtime, txId, root, R"(
                Name: "x/y/z"
            )");
        });
    }

    Y_UNIT_TEST(CreateSolomonWithIntermediateDirsForceDrop) {
        CreateWithIntermediateDirsForceDrop([](TTestActorRuntime& runtime, ui64 txId, const TString& root) {
            AsyncCreateSolomon(runtime, txId, root, R"(
                Name: "x/y/z"
                PartitionCount: 2
            )");
        });
    }

    Y_UNIT_TEST(CreateSubDomainWithIntermediateDirs) {
        const TString validScheme = R"(
            Name: "Valid/x/y/z"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
        )";
        const TString invalidScheme = R"(
            Name: "Invalid/wr0ng n@me"
        )";
        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusSchemeError;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestCreateSubDomain(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateWithIntermediateDirs) {
        const TString validScheme = R"(
            Name: "Valid/x/y/z"
            PartitionsCount: 0
        )";
        const TString invalidScheme = R"(
            Name: "Invalid/wr0ng n@me"
        )";
        const auto validStatus = NKikimrScheme::StatusAccepted;
        const auto invalidStatus = NKikimrScheme::StatusSchemeError;

        CreateWithIntermediateDirs([&](TTestActorRuntime& runtime, ui64 txId, const TString& root, bool valid) {
            TestCreateRtmrVolume(runtime, txId, root, valid ? validScheme : invalidScheme, {valid ? validStatus : invalidStatus});
        });
    }

    Y_UNIT_TEST(CreateWithIntermediateDirsForceDrop) {
        CreateWithIntermediateDirsForceDrop([](TTestActorRuntime& runtime, ui64 txId, const TString& root) {
            AsyncCreateRtmrVolume(runtime, txId, root, R"(
                Name: "x/y/z"
                PartitionsCount: 0
            )");
        });
    }
}

Y_UNIT_TEST_SUITE(TConsistentOpsWithReboots) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST(CopyWithData) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion pathVersion;
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "DirB");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirB",
                                "Name: \"src1\""
                                "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                "UniformPartitionsCount: 1"
                                );
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirB",
                                "Name: \"src2\""
                                "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                "UniformPartitionsCount: 1"
                                );
                t.TestEnv->TestWaitNotification(runtime, {t.TxId-2, t.TxId-1, t.TxId});

                // Write some data to the user table
                auto fnWriteRow = [&] (ui64 tabletId) {
                    TString writeQuery = R"(
                        (
                            (let key '( '('key1 (Uint32 '0)) '('key2 (Utf8 'aaaa)) '('key3 (Uint64 '0)) ) )
                            (let value '('('Value (Utf8 '281474980010683)) ) )
                            (return (AsList (UpdateRow '__user__src1 key value) ))
                        )
                    )";
                    NKikimrMiniKQL::TResult result;
                    TString err;
                    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                    UNIT_ASSERT_VALUES_EQUAL(err, "");
                    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
                };
                fnWriteRow(TTestTxConfig::FakeHiveTablets);

                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                                 {NLs::PathVersionEqual(7)});
            }

            t.TestEnv->ReliablePropose(runtime, ConsistentCopyTablesRequest(++t.TxId, "/", R"(
                           CopyTableDescriptions {
                             SrcPath: "/MyRoot/DirB/src1"
                             DstPath: "/MyRoot/DirB/dst1"
                           }
                          CopyTableDescriptions {
                            SrcPath: "/MyRoot/DirB/src2"
                            DstPath: "/MyRoot/DirB/dst2"
                          }
                    )", {pathVersion}),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                   {NLs::PathVersionEqual(11),
                                    NLs::ChildrenCount(4)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/src1"),
                                   {NLs::PathVersionEqual(3),
                                    NLs::IsTable});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/src2"),
                                   {NLs::PathVersionEqual(3),
                                    NLs::IsTable});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/dst1"),
                                   {NLs::PathVersionEqual(3),
                                    NLs::IsTable});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/dst2"),
                                   {NLs::PathVersionEqual(3),
                                    NLs::IsTable});
            }
        });
    }

    Y_UNIT_TEST(DropWithData) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "DirB");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirB",
                                "Name: \"src1\""
                                "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                "UniformPartitionsCount: 1"
                                );
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirB",
                                "Name: \"src2\""
                                "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                "UniformPartitionsCount: 1"
                                );
                t.TestEnv->TestWaitNotification(runtime, {t.TxId-2, t.TxId-1, t.TxId});

                // Write some data to the user table
                auto fnWriteRow = [&] (ui64 tabletId) {
                    TString writeQuery = R"(
                        (
                            (let key '( '('key1 (Uint32 '0)) '('key2 (Utf8 'aaaa)) '('key3 (Uint64 '0)) ) )
                            (let value '('('Value (Utf8 '281474980010683)) ) )
                            (return (AsList (UpdateRow '__user__src1 key value) ))
                        )
                    )";
                    NKikimrMiniKQL::TResult result;
                    TString err;
                    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                    UNIT_ASSERT_VALUES_EQUAL(err, "");
                    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
                };
                fnWriteRow(TTestTxConfig::FakeHiveTablets);

                TestConsistentCopyTables(runtime, ++t.TxId, "/", R"(
                           CopyTableDescriptions {
                             SrcPath: "/MyRoot/DirB/src1"
                             DstPath: "/MyRoot/DirB/dst1"
                           }
                          CopyTableDescriptions {
                            SrcPath: "/MyRoot/DirB/src2"
                            DstPath: "/MyRoot/DirB/dst2"
                          }
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            AsyncDropTable(runtime, ++t.TxId, "/MyRoot/DirB", "src1");
            AsyncDropTable(runtime, ++t.TxId, "/MyRoot/DirB", "dst1");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 2});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                   {NLs::PathVersionEqual(15),
                                    NLs::ChildrenCount(2)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/src1"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/src2"),
                                   {NLs::PathVersionEqual(3),
                                    NLs::IsTable});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/dst1"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/dst2"),
                                   {NLs::PathVersionEqual(3),
                                    NLs::IsTable});
            }
        });
    }

    Y_UNIT_TEST(CreateIndexedTableWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncMkDir(runtime, ++t.TxId, "/MyRoot", "DirB");
            AsyncCreateIndexedTable(runtime, ++t.TxId, "/MyRoot/DirB", R"(
                TableDescription {
                  Name: "Table1"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value0" Type: "Utf8" }
                  Columns { Name: "value1" Type: "Utf8" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue0"
                  KeyColumnNames: ["value0"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValues"
                  KeyColumnNames: ["value0", "value1"]
                  State: EIndexStateNotReady
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue0CoveringValue1"
                  KeyColumnNames: ["value0"]
                  DataColumnNames: ["value1"]
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                   { NLs::Finished,
                                    NLs::PathVersionEqual(5),
                                    NLs::ChildrenCount(1)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/Table1"),
                                   { NLs::Finished,
                                    NLs::PathVersionEqual(3),
                                    NLs::IndexesCount(3)});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirB/Table1/UserDefinedIndexByValue0"),
                                   {NLs::Finished,
                                    NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                                    NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                                    NLs::IndexKeys({"value0"})});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirB/Table1/UserDefinedIndexByValue0/indexImplTable"),
                                   {NLs::Finished});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirB/Table1/UserDefinedIndexByValues"),
                                   {NLs::Finished,
                                    NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                                    NLs::IndexState(NKikimrSchemeOp::EIndexStateNotReady),
                                    NLs::IndexKeys({"value0", "value1"})});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirB/Table1/UserDefinedIndexByValues/indexImplTable"),
                                   {NLs::Finished});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirB/Table1/UserDefinedIndexByValue0CoveringValue1"),
                                   {NLs::Finished,
                                    NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                                    NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                                    NLs::IndexKeys({"value0"}),
                                    NLs::IndexDataColumns({"value1"})});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirB/Table1/UserDefinedIndexByValue0CoveringValue1/indexImplTable"),
                                   {NLs::Finished});

            }

        });
    }

    Y_UNIT_TEST(DropIndexedTableWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                AsyncMkDir(runtime, ++t.TxId, "/MyRoot", "DirB");
                AsyncCreateIndexedTable(runtime, ++t.TxId, "/MyRoot/DirB", R"(
                    TableDescription {
                      Name: "Table1"
                      Columns { Name: "key"   Type: "Uint64" }
                      Columns { Name: "value0" Type: "Utf8" }
                      Columns { Name: "value1" Type: "Utf8" }
                      KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                      Name: "UserDefinedIndexByValue0"
                      KeyColumnNames: ["value0"]
                    }
                    IndexDescription {
                      Name: "UserDefinedIndexByValues"
                      KeyColumnNames: ["value0", "value1"]
                    }
                    IndexDescription {
                      Name: "UserDefinedIndexByValue0CoveringValue1"
                      KeyColumnNames: ["value0"]
                      DataColumnNames: ["value1"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});
            }


            TestDropTable(runtime, ++t.TxId, "/MyRoot/DirB", "Table1");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                   {NLs::Finished,
                                    NLs::PathVersionEqual(7),
                                    NLs::ChildrenCount(0)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/Table1"),
                                   {NLs::PathNotExist});
                t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 3));
            }
        });
    }

    Y_UNIT_TEST(CreateIndexedTableAndForceDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion dirAVersion;

            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", "DirB");
                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot/DirB", R"(
                    TableDescription {
                      Name: "Table1"
                      Columns { Name: "key"   Type: "Uint64" }
                      Columns { Name: "value0" Type: "Utf8" }
                      Columns { Name: "value1" Type: "Utf8" }
                      KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                      Name: "UserDefinedIndexByValue0"
                      KeyColumnNames: ["value0"]
                    }
                    IndexDescription {
                      Name: "UserDefinedIndexByValue1"
                      KeyColumnNames: ["value1"]
                    }
                )");

                t.TestEnv->TestWaitNotification(runtime, {t.TxId - 2, t.TxId - 1, t.TxId});

                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::Finished,
                                    NLs::ChildrenCount(2)});

                dirAVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                                 {NLs::Finished,
                                                  NLs::PathVersionEqual(5),
                                                  NLs::ChildrenCount(1)});
            }

            TestForceDropUnsafe(runtime, ++t.TxId, dirAVersion.PathId.LocalPathId);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 10));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::Finished,
                                    NLs::ChildrenCount(1)});
            }
        });
    }

    Y_UNIT_TEST(CreateIndexedTableAndForceDropSimultaneously) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion dirAVersion;
            {
                TInactiveZone inactive(activeZone);
                dirAVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                                 {NLs::Finished,
                                                  NLs::PathVersionEqual(3),
                                                  NLs::ChildrenCount(0)});
            }

            AsyncCreateIndexedTable(runtime, ++t.TxId, "/MyRoot/DirA", R"(
                                    TableDescription {
                                      Name: "Table1"
                                      Columns { Name: "key"   Type: "Uint64" }
                                      Columns { Name: "value0" Type: "Utf8" }
                                      Columns { Name: "value1" Type: "Utf8" }
                                      KeyColumnNames: ["key"]
                                    }
                                    IndexDescription {
                                      Name: "UserDefinedIndexByValue0"
                                      KeyColumnNames: ["value0"]
                                    }
                                    IndexDescription {
                                      Name: "UserDefinedIndexByValue1"
                                      KeyColumnNames: ["value1"]
                                    })");
            AsyncForceDropUnsafe(runtime, ++t.TxId, dirAVersion.PathId.LocalPathId);
            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 10));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::Finished,
                                    NLs::ChildrenCount(0)});
            }
        });
    }

    Y_UNIT_TEST(DropIndexedTableAndForceDropSimultaneously) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion dirAVersion;

            {
                TInactiveZone inactive(activeZone);
                AsyncMkDir(runtime, ++t.TxId, "/MyRoot", "DirB");
                AsyncCreateIndexedTable(runtime, ++t.TxId, "/MyRoot/DirB", R"(
                                            TableDescription {
                                              Name: "Table1"
                                              Columns { Name: "key"   Type: "Uint64" }
                                              Columns { Name: "value0" Type: "Utf8" }
                                              Columns { Name: "value1" Type: "Utf8" }
                                              KeyColumnNames: ["key"]
                                            }
                                            IndexDescription {
                                              Name: "UserDefinedIndexByValue0"
                                              KeyColumnNames: ["value0"]
                                            }
                                            IndexDescription {
                                              Name: "UserDefinedIndexByValue1"
                                              KeyColumnNames: ["value1"]
                                            }
                                            IndexDescription {
                                                Name: "UserDefinedIndexByValue0CoveringValue1"
                                                KeyColumnNames: ["value0"]
                                                DataColumnNames: ["value1"]
                                            }
                                        )");

                t.TestEnv->TestWaitNotification(runtime, {t.TxId - 2, t.TxId - 1, t.TxId});

                dirAVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                                 {NLs::Finished,
                                                  NLs::PathVersionEqual(5),
                                                  NLs::ChildrenCount(1)});
            }

            TestDropTable(runtime, ++t.TxId, "/MyRoot/DirB", "Table1");

            TestForceDropUnsafe(runtime, ++t.TxId, dirAVersion.PathId.LocalPathId);

            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 10));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::Finished,
                                    NLs::PathVersionOneOf({8, 9}),
                                    NLs::ChildrenCount(1)});
            }
        });
    }

    Y_UNIT_TEST(CreateNotNullColumnTableWithReboots) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnableNotNullDataColumns(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TestMkDir(runtime, ++t.TxId, "/MyRoot", "DirB");
            TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirB", R"(
                Name: "TestNotNullTable"
                Columns { Name: "key" Type: "Uint64" NotNull: true}
                Columns { Name: "value" Type: "Utf8" NotNull: true}
                KeyColumnNames: ["key"]
            )");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                   { NLs::Finished,
                                     NLs::ChildrenCount(1) });
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/TestNotNullTable"),
                                   { NLs::Finished,
                                     NLs::PathExist });
            }
        });
    }

    Y_UNIT_TEST(DropNotNullColumnTableWithReboots) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnableNotNullDataColumns(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "DirB");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirB", R"(
                    Name: "TestNotNullTable"
                    Columns { Name: "key" Type: "Uint64" NotNull: true}
                    Columns { Name: "value" Type: "Utf8" NotNull: true}
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});
            }

            TestDropTable(runtime, ++t.TxId, "/MyRoot/DirB", "TestNotNullTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 10});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                   { NLs::Finished,
                                     NLs::ChildrenCount(0) });
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/TestNotNullTable"),
                                   { NLs::PathNotExist });
            }
        });
    }
}

Y_UNIT_TEST_SUITE(TSolomonReboots) {
    Y_UNIT_TEST(CreateDropSolomonWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TestCreateSolomon(runtime, ++t.TxId, "/MyRoot", R"(
                                                          Name: "Solomon"
                                                          PartitionCount: 2)");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "MyRoot/Solomon"),
                                   {NLs::Finished,
                                    NLs::PathExist});
            }

            ++t.TxId;
            TestDropSolomon(runtime, ++t.TxId, "/MyRoot", "Solomon");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "MyRoot/Solomon"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(AdoptDropSolomonWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            NKikimrSchemeOp::TCreateSolomonVolume volumeDescr;
            {
                TInactiveZone inactive(activeZone);
                TestCreateSolomon(runtime, ++t.TxId, "/MyRoot", R"(
                                    Name: "JunkSolomon"
                                    PartitionCount: 2)");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                auto descr = DescribePath(runtime, "/MyRoot/JunkSolomon");
                TestDescribeResult(descr,
                                   {NLs::PathExist});
                volumeDescr = TakeTabletsFromAnotherSolomonVol("Solomon", descr.DebugString());
            }

            TestCreateSolomon(runtime, ++t.TxId, "/MyRoot", volumeDescr.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "MyRoot/Solomon"),
                                   {NLs::Finished,
                                    NLs::PathExist});
            }

            TestDropSolomon(runtime, ++t.TxId, "/MyRoot", "Solomon");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "MyRoot/Solomon"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(CreateAlterSolomonWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            NKikimrSchemeOp::TCreateSolomonVolume volumeDescr;
            {
                TestCreateSolomon(runtime, ++t.TxId, "/MyRoot", R"(
                                                              Name: "Solomon"
                                                              PartitionCount: 2)");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            ++t.TxId;
            TestAlterSolomon(runtime, ++t.TxId, "/MyRoot", R"(
                                                              Name: "Solomon"
                                                              PartitionCount: 4
                                                              ChannelProfileId: 0)");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "MyRoot/Solomon"),
                                   {NLs::Finished,
                                    NLs::PathExist,
                                    NLs::ShardsInsideDomain(4)});
            }

            ++t.TxId;
            TestDropSolomon(runtime, ++t.TxId, "/MyRoot", "Solomon");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 4));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "MyRoot/Solomon"),
                                   {NLs::PathNotExist});
            }
        });
    }
}

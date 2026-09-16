#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TTablesWithReboots) {
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateWithRebootsAtCommit, 2, 1, true) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            t.TestEnv->ReliablePropose(runtime, CreateTableRequest(++t.TxId,
                                                                   "/MyRoot",
                                                                   "Name: \"Table\""
                                                                   "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                                                   "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                                                   "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                                                   "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                                                   "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                                                   "UniformPartitionsCount: 2"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                   {NLs::Finished,
                                    NLs::IsTable,
                                    NLs::PartitionCount(2)});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CopyWithRebootsAtCommit, 2, 1, true) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 expectedDomainPaths;
            {
                TInactiveZone inactive(activeZone);
                auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
                expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

                t.TestEnv->ReliablePropose(runtime, CreateTableRequest(++t.TxId,
                                                                       "/MyRoot",
                                                                       "Name: \"Table\""
                                                                       "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                                                       "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                                                       "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                                                       "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                                                       "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                                                       "UniformPartitionsCount: 2"),
                                           {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                expectedDomainPaths += 1;
            }

            t.TestEnv->ReliablePropose(runtime, CopyTableRequest(++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            expectedDomainPaths += 1;

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(4)});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable", true),
                                   {NLs::Finished,
                                    NLs::IsTable,
                                    NLs::PartitionCount(2),
                                    NLs::ShardsInsideDomain(4),
                                    NLs::PathsInsideDomain(expectedDomainPaths)});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropCopyWithRebootsAtCommit, 2, 1, true) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId,
                                           "/MyRoot",
                                           "Name: \"Table\""
                                           "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                           "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                           "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                           "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                           "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                           "UniformPartitionsCount: 1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCopyTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime, DropTableRequest(++t.TxId, "/MyRoot", "Table"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->ReliablePropose(runtime, DropTableRequest(++t.TxId, "/MyRoot", "NewTable"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+4));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(TwiceRmDirWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "Victim");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime, RmDirRequest(++t.TxId, "/MyRoot", "Victim"));
            AsyncRmDir(runtime, ++t.TxId, "/MyRoot", "Victim");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Victim"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateTableWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncCreateTable(runtime, ++t.TxId, "/MyRoot/DirA",
                             "Name: \"Table1\""
                             "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                             "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                             "KeyColumnNames: [\"RowId\"]");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Table1"),
                                   {NLs::Finished,
                                    NLs::IsTable});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ParallelCreateDrop, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                            Name: "DropMe"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                            UniformPartitionsCount: 2
                        )");
            AsyncDropTable(runtime, ++t.TxId, "/MyRoot", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);


            TestDropTable(runtime, ++t.TxId, "/MyRoot", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+4));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DropMe"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(SimpleDropTableWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"Table\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]"
                                "UniformPartitionsCount: 1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(SimpleDropTableWithReboots2, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"Table\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]"
                                "UniformPartitionsCount: 2");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(DropTableWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, t.TxId, "/MyRoot",
                                "Name: \"Table\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]"
                                "UniformPartitionsCount: 2");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});

                TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"Table\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]"
                                "UniformPartitionsCount: 3");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});
            }

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 7));
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateDroppedTableWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"Table\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]"
                                "UniformPartitionsCount: 2");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                            "Name: \"Table\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                            "UniformPartitionsCount: 3");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 7));
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CreateDroppedTableAndDropWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"Table\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]"
                                "UniformPartitionsCount: 2");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"Table\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]"
                                "UniformPartitionsCount: 3");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});
            }

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 7));
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(AlterTableSchemaWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Prepare the table

            TSet<TString> cols = {"key1", "key2", "value"};
            TSet<TString> keyCols = {"key1", "key2"};
            TSet<TString> dropCols;

            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key2"   Type: "Uint32"}
                                Columns { Name: "key1"   Type: "Uint64"}
                                Columns { Name: "value"  Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                UniformPartitionsCount: 2
                                PartitionConfig {
                                    TxReadSizeLimit: 100
                                    ExecutorCacheSize: 42
                                }
                            )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Start altering the table

            cols.erase("value");
            dropCols.insert("value");
            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                DropColumns { Name: "value" }
                           )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::IsTable,
                                    NLs::CheckColumns("Table", cols, dropCols, keyCols)});
            }

            cols.insert("add_2");
            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "add_2"  Type: "Uint64"}
                           )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::IsTable,
                                    NLs::CheckColumns("Table", cols, dropCols, keyCols)});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(AlterTableSchemaFreezeUnfreezeWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key2"   Type: "Uint32"}
                                Columns { Name: "key1"   Type: "Uint64"}
                                Columns { Name: "value"  Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                UniformPartitionsCount: 2
                                PartitionConfig {
                                    TxReadSizeLimit: 100
                                    ExecutorCacheSize: 42
                                })");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                           Name: "Table"
                           PartitionConfig { FreezeState: Freeze }
                       )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "MyRoot/Table", true),
                                   {NLs::Finished,
                                    NLs::FreezeStateEqual(NKikimrSchemeOp::EFreezeState::Freeze)});
            }

            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                           Name: "Table"
                           PartitionConfig { FreezeState: Unfreeze }
                      )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "MyRoot/Table", true),
                                   {NLs::Finished,
                                    NLs::FreezeStateEqual(NKikimrSchemeOp::EFreezeState::Unfreeze)});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(AlterTableFollowersWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, t.TxId, "/MyRoot", R"(
                                    Name: "Table"
                                    Columns { Name: "key"   Type: "Uint64"}
                                    Columns { Name: "value"  Type: "Utf8"}
                                    KeyColumnNames: ["key"])");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Start altering the table

            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    FollowerCount: 2
                                    AllowFollowerPromotion: true
                                }
                            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            // TODO: check followers

            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    FollowerCount: 1
                                    AllowFollowerPromotion: false
                                }
                           )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            // TODO: check followers

            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    FollowerCount: 0
                                    AllowFollowerPromotion: true
                                }
                           )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            // TODO: check followers
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(AlterTableConfigWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 datashardTabletId = ui64(InvalidTabletId);
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key2"   Type: "Uint32"}
                                Columns { Name: "key1"   Type: "Uint64"}
                                Columns { Name: "value"  Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                PartitionConfig {
                                    TxReadSizeLimit: 100
                                    ExecutorCacheSize: 42
                                })");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);


                datashardTabletId = TTestTxConfig::FakeHiveTablets;
                UNIT_ASSERT_VALUES_EQUAL(GetTxReadSizeLimit(runtime, datashardTabletId), 100);
                UNIT_ASSERT_VALUES_EQUAL(GetExecutorCacheSize(runtime, datashardTabletId), 42);
            }


            // Start altering the table
            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    TxReadSizeLimit: 2000
                                })");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                UNIT_ASSERT_VALUES_EQUAL(GetTxReadSizeLimit(runtime, datashardTabletId), 2000);
                UNIT_ASSERT_VALUES_EQUAL(GetExecutorCacheSize(runtime, datashardTabletId), 42);
            }

            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "add_2"  Type: "Uint64"}
                                PartitionConfig {
                                    ExecutorCacheSize: 100500
                                })");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                UNIT_ASSERT_VALUES_EQUAL(GetTxReadSizeLimit(runtime, datashardTabletId), 2000);
                UNIT_ASSERT_VALUES_EQUAL(GetExecutorCacheSize(runtime, datashardTabletId), 100500);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(AlterCopyWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key2"   Type: "Uint32"}
                                Columns { Name: "key1"   Type: "Uint64"}
                                Columns { Name: "value"  Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                PartitionConfig {
                                    TxReadSizeLimit: 100
                                    ExecutorCacheSize: 42
                                })");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Start altering the table
            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    TxReadSizeLimit: 2000
                                })");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestCopyTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "NewTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 2));
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CopyAlterWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key2"   Type: "Uint32"}
                                Columns { Name: "key1"   Type: "Uint64"}
                                Columns { Name: "value"  Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                UniformPartitionsCount: 1
                                PartitionConfig {
                                    TxReadSizeLimit: 100
                                    ExecutorCacheSize: 42
                                })");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Start altering the table
            TestCopyTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "NewTable"
                                Columns { Name: "add_2"  Type: "Uint64"}
                                PartitionConfig {
                                    ExecutorCacheSize: 100500
                                })");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDropTable(runtime, ++t.TxId, "/MyRoot", "NewTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 4));
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(AlterAndForceDrop, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion pathVersion;
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirA", R"(
                                Name: "Table"
                                Columns { Name: "key2"   Type: "Uint32"}
                                Columns { Name: "key1"   Type: "Uint64"}
                                Columns { Name: "value"  Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                UniformPartitionsCount: 1
                                PartitionConfig {
                                    TxReadSizeLimit: 100
                                    ExecutorCacheSize: 42
                                })");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA")
                                                     , {NLs::ChildrenCount(1)});
            }

            TestAlterTable(runtime, ++t.TxId, "/MyRoot/DirA", R"(
                                Name: "Table"
                                Columns { Name: "add_2"  Type: "Uint64"}
                                PartitionConfig {
                                    ExecutorCacheSize: 100500
                                })");
            TestForceDropUnsafe(runtime, ++t.TxId, pathVersion.PathId.LocalPathId);
            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});

            {
                TInactiveZone inactive(activeZone);
                t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 1));
                TestDescribeResult(DescribePath(runtime, "/MyRoot")
                                       , {NLs::ChildrenCount(1)});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CopyTableWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Uint32"}
                                Columns { Name: "key2"       Type: "Utf8"}
                                Columns { Name: "key3"       Type: "Uint64"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2", "key3"]
                                UniformPartitionsCount: 2)");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCopyTable(runtime, ++t.TxId, "/MyRoot", "NewTable1", "/MyRoot/Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }


            TestCopyTable(runtime, ++t.TxId, "/MyRoot", "NewTable2", "/MyRoot/NewTable1");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "MyRoot"),
                                   {NLs::ChildrenCount(5)});
                TestDescribeResult(DescribePath(runtime, "MyRoot/Table"),
                                   {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, "MyRoot/NewTable1"),
                                   {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, "MyRoot/NewTable2"),
                                   {NLs::PathExist});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CopyIndexedTableWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", R"(
                    TableDescription {
                        Name: "Table"
                        Columns { Name: "key1"       Type: "Uint32"}
                        Columns { Name: "key2"       Type: "Utf8"}
                        Columns { Name: "key3"       Type: "Uint64"}
                        Columns { Name: "value0"      Type: "String"}
                        Columns { Name: "value1"      Type: "String"}
                        KeyColumnNames: ["key1", "key2", "key3"]
                        UniformPartitionsCount: 2
                    }
                    IndexDescription {
                      Name: "VectorIndexByValue0CoveringValue1"
                      Type: EIndexTypeGlobalVectorKmeansTree
                      KeyColumnNames: ["value0"]
                      DataColumnNames: ["value1"]
                      VectorIndexKmeansTreeDescription {
                        Settings {
                          settings {
                            metric: DISTANCE_COSINE
                            vector_type: VECTOR_TYPE_INT8
                            vector_dimension: 2
                          }
                          clusters: 3
                          levels: 5
                        }
                      }
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCopyTable(runtime, ++t.TxId, "/MyRoot", "NewTable1", "/MyRoot/Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }


            TestCopyTable(runtime, ++t.TxId, "/MyRoot", "NewTable2", "/MyRoot/NewTable1");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "MyRoot"),
                                   {NLs::ChildrenCount(5)});
                TestDescribeResult(DescribePath(runtime, "MyRoot/Table"),
                                   {NLs::PathExist, NLs::IndexesCount(1)});
                TestDescribeResult(DescribePath(runtime, "MyRoot/NewTable1"),
                                   {NLs::PathExist, NLs::IndexesCount(1)});
                TestDescribeResult(DescribePath(runtime, "MyRoot/NewTable2"),
                                   {NLs::PathExist, NLs::IndexesCount(1)});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CopyTableAndDropWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"Table\""
                                "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCopyTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDropTable(runtime, ++t.TxId, "/MyRoot", "NewTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 4));
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CopyTableAndDropWithReboots2, 4, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"Table\""
                                "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Write some data to the user table
                auto fnWriteRow = [&] (ui64 tabletId) {
                    TString writeQuery = R"(
                    (
                        (let key '( '('key1 (Uint32 '0)) '('key2 (Utf8 'aaaa)) '('key3 (Uint64 '0)) ) )
                        (let value '('('Value (Utf8 '281474980010683)) ) )
                        (return (AsList (UpdateRow '__user__Table key value) ))
                    ))";
                    NKikimrMiniKQL::TResult result;
                    TString err;
                    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                    UNIT_ASSERT_VALUES_EQUAL(err, "");
                    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
                };
                fnWriteRow(TTestTxConfig::FakeHiveTablets);
            }

            TestCopyTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            AsyncDropTable(runtime, ++t.TxId, "/MyRoot", "Table");
            AsyncDropTable(runtime, ++t.TxId, "/MyRoot", "NewTable");
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 2));
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ChainedCopyTableAndDropWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const int maxTableIdx = 4;

            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"Table1\""
                                "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Write some data to the user table
                auto fnWriteRow = [&] (ui64 tabletId) {
                    TString writeQuery = R"(
                        (
                            (let key '( '('key1 (Uint32 '0)) '('key2 (Utf8 'aaaa)) '('key3 (Uint64 '0)) ) )
                            (let value '('('Value (Utf8 '281474980010683)) ) )
                            (return (AsList (UpdateRow '__user__Table1 key value) ))
                        )
                    )";
                    NKikimrMiniKQL::TResult result;
                    TString err;
                    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                    UNIT_ASSERT_VALUES_EQUAL(err, "");
                    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
                };
                fnWriteRow(TTestTxConfig::FakeHiveTablets);

                // Make a chain of copy-of-copy
                for (int i = 2; i <= maxTableIdx; ++i) {
                    TestCopyTable(runtime, ++t.TxId, "/MyRoot", Sprintf("Table%d", i), Sprintf("/MyRoot/Table%d", i-1));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }

                // Drop all intermediate copies
                for (int i = 1; i < maxTableIdx; ++i) {
                    TestDropTable(runtime, ++t.TxId, "/MyRoot", Sprintf("Table%d", i));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
            }

            // Drop the last table to trigger chained parts return
            TestDropTable(runtime, ++t.TxId, "/MyRoot", Sprintf("Table%d", maxTableIdx));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 9));
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(LostBorrowAckWithReboots, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"Table1\""
                                "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Write some data to the user table
                auto fnWriteRow = [&] (ui64 tabletId) {
                    TString writeQuery = R"(
                        (
                            (let key '( '('key1 (Uint32 '0)) '('key2 (Utf8 'aaaa)) '('key3 (Uint64 '0)) ) )
                            (let value '('('Value (Utf8 '281474980010683)) ) )
                            (return (AsList (UpdateRow '__user__Table1 key value) ))
                        )
                    )";
                    NKikimrMiniKQL::TResult result;
                    TString err;
                    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                    UNIT_ASSERT_VALUES_EQUAL(err, "");
                    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
                };
                fnWriteRow(TTestTxConfig::FakeHiveTablets);

                TestCopyTable(runtime, ++t.TxId, "/MyRoot", "Table2", "/MyRoot/Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Drop BorrowAcks events
            auto prevFilter = runtime.SetEventFilter(nullptr);
            auto borrowAckFilter = [prevFilter](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) -> bool {
                if (event->Type == TEvDataShard::EvReturnBorrowedPartAck &&
                    event->Sender != event->Recipient)  // only allow the event from Self that are used for auto-Ack
                {
                    // Cerr << "     DROPPED BORROW ACK\n";
                    return true;
                }
                return prevFilter(runtime, event);
            };
            runtime.SetEventFilter(borrowAckFilter);

            // Drop the last table to trigger parts return
            TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table2");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 2));
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(SimultaneousDropForceDrop, 2, 1, false) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 expectedDomainPaths;
            {
                TInactiveZone inactive(activeZone);
                auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
                expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();
            }

            TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            expectedDomainPaths += 1;

            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::ChildrenCount(3)});
            auto pathVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                                              {NLs::PathExist,
                                               NLs::PathVersionEqual(3)});

            TestDropTable(runtime, ++t.TxId,  "/MyRoot", "Table1");

            AsyncForceDropUnsafe(runtime, ++t.TxId,  pathVer.PathId.LocalPathId);

            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});
            expectedDomainPaths -= 1;
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 2));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(2),
                                    NLs::PathsInsideDomain(expectedDomainPaths),
                                    NLs::ShardsInsideDomainOneOf({0,1})});
            }
        });
    }

    Y_UNIT_TEST(IncrementalBackupConfigPersistsAfterReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        // Create table and set up continuous backup
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            ContinuousBackupDescription {
                StreamName: "0_continuousBackupImpl"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Take incremental backup - creates IncrBackupImpl table with IncrementalBackupConfig
        TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            TakeIncrementalBackup {
                DstPath: "IncrBackupImpl"
                DstStreamPath: "1_continuousBackupImpl"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Verify IncrBackupImpl table has IncrementalBackupConfig before reboot
        {
            auto describe = DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl");
            const auto& tableDesc = describe.GetPathDescription().GetTable();
            UNIT_ASSERT(tableDesc.HasIncrementalBackupConfig());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(tableDesc.GetIncrementalBackupConfig().GetMode()),
                static_cast<int>(NKikimrSchemeOp::TTableIncrementalBackupConfig::RESTORE_MODE_INCREMENTAL_BACKUP)
            );
        }

        // Reboot SchemeShard
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Verify IncrementalBackupConfig survives reboot
        {
            auto describe = DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl");
            const auto& tableDesc = describe.GetPathDescription().GetTable();
            UNIT_ASSERT_C(tableDesc.HasIncrementalBackupConfig(),
                "IncrementalBackupConfig lost after SchemeShard reboot");
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(tableDesc.GetIncrementalBackupConfig().GetMode()),
                static_cast<int>(NKikimrSchemeOp::TTableIncrementalBackupConfig::RESTORE_MODE_INCREMENTAL_BACKUP)
            );
        }
    }

    // Base-stat fields sent to the Statistics Aggregator (RowCount, RowUpdates,
    // RowDeletes) are persisted in the schemeshard local DB and must survive a reboot.
    Y_UNIT_TEST_WITH_REBOOTS(BaseStatsSurviveReboot) {
        t.GetTestEnvOptions().EnablePersistentPartitionStats(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            constexpr ui32 WrittenRows = 50;
            constexpr ui32 DeletedRows = 10;

            ui64 rowCountBefore = 0;
            ui64 rowUpdatesBefore = 0;
            ui64 rowDeletesBefore = 0;

            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Simple"
                    Columns { Name: "key"   Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (ui32 key = 0; key < WrittenRows; ++key) {
                    WriteRow(runtime, ++t.TxId, "/MyRoot/Simple", 0, key, "value");
                }
                for (ui32 key = 0; key < DeletedRows; ++key) {
                    DeleteRow(runtime, ++t.TxId, "/MyRoot/Simple", 0, key);
                }

                while (rowUpdatesBefore < WrittenRows || rowDeletesBefore < DeletedRows) {
                    auto describe = DescribePrivatePath(runtime, "/MyRoot/Simple", true, true);
                    const auto& stats = describe.GetPathDescription().GetTableStats();
                    rowCountBefore = stats.GetRowCount();
                    rowUpdatesBefore = stats.GetRowUpdates();
                    rowDeletesBefore = stats.GetRowDeletes();
                    t.TestEnv->SimulateSleep(runtime, TDuration::Seconds(1));
                }
            }

            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

            {
                TInactiveZone inactive(activeZone);

                auto describe = DescribePrivatePath(runtime, "/MyRoot/Simple", true, true);
                const auto& stats = describe.GetPathDescription().GetTableStats();

                // All three fields are persisted in the schemeshard local DB and
                // must be restored to the same values after the reboot.
                UNIT_ASSERT_VALUES_EQUAL(stats.GetRowCount(), rowCountBefore);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetRowUpdates(), rowUpdatesBefore);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetRowDeletes(), rowDeletesBefore);
            }
        });
    }

    // Without EnablePersistentPartitionStats, schemeshard loses in-memory base stats on
    // reboot. Datashards keep their counters and re-send them via PeriodicTableStats,
    // so the table aggregate must recover after some time.
    Y_UNIT_TEST_WITH_REBOOTS(BaseStatsRecoverAfterRebootWithoutPersistence) {
        t.GetTestEnvOptions()
            .EnablePersistentPartitionStats(false)
            .DataShardStatsReportIntervalSeconds(1);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            constexpr ui32 WrittenRows = 50;
            constexpr ui32 DeletedRows = 10;

            ui64 rowCountBefore = 0;
            ui64 rowUpdatesBefore = 0;
            ui64 rowDeletesBefore = 0;

            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Simple"
                    Columns { Name: "key"   Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (ui32 key = 0; key < WrittenRows; ++key) {
                    WriteRow(runtime, ++t.TxId, "/MyRoot/Simple", 0, key, "value");
                }
                for (ui32 key = 0; key < DeletedRows; ++key) {
                    DeleteRow(runtime, ++t.TxId, "/MyRoot/Simple", 0, key);
                }

                while (rowUpdatesBefore < WrittenRows || rowDeletesBefore < DeletedRows) {
                    auto describe = DescribePrivatePath(runtime, "/MyRoot/Simple", true, true);
                    const auto& stats = describe.GetPathDescription().GetTableStats();
                    rowCountBefore = stats.GetRowCount();
                    rowUpdatesBefore = stats.GetRowUpdates();
                    rowDeletesBefore = stats.GetRowDeletes();
                    t.TestEnv->SimulateSleep(runtime, TDuration::Seconds(1));
                }

                // Reboot only schemeshard; leave the datashard running so its in-memory
                // RowUpdates/RowDeletes survive and can be re-reported.
                TActorId sender = runtime.AllocateEdgeActor();
                RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

                // Stats were not persisted, so they start empty after the reboot.
                {
                    auto describe = DescribePrivatePath(runtime, "/MyRoot/Simple", true, true);
                    const auto& stats = describe.GetPathDescription().GetTableStats();
                    UNIT_ASSERT_VALUES_EQUAL(stats.GetRowCount(), 0u);
                    UNIT_ASSERT_VALUES_EQUAL(stats.GetRowUpdates(), 0u);
                    UNIT_ASSERT_VALUES_EQUAL(stats.GetRowDeletes(), 0u);
                }

                ui64 rowCountAfter = 0;
                ui64 rowUpdatesAfter = 0;
                ui64 rowDeletesAfter = 0;
                while (rowCountAfter < rowCountBefore
                        || rowUpdatesAfter < rowUpdatesBefore
                        || rowDeletesAfter < rowDeletesBefore)
                {
                    auto describe = DescribePrivatePath(runtime, "/MyRoot/Simple", true, true);
                    const auto& stats = describe.GetPathDescription().GetTableStats();
                    rowCountAfter = stats.GetRowCount();
                    rowUpdatesAfter = stats.GetRowUpdates();
                    rowDeletesAfter = stats.GetRowDeletes();
                    t.TestEnv->SimulateSleep(runtime, TDuration::Seconds(1));
                }

                UNIT_ASSERT_VALUES_EQUAL(rowCountAfter, rowCountBefore);
                UNIT_ASSERT_VALUES_EQUAL(rowUpdatesAfter, rowUpdatesBefore);
                UNIT_ASSERT_VALUES_EQUAL(rowDeletesAfter, rowDeletesBefore);
            }
        });
    }
}

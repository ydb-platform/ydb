#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TExternalTableTestReboots) {
    Y_UNIT_TEST(CreateExternalTableWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncMkDir(runtime, ++t.TxId, "/MyRoot", "DirExternalTable");

            AsyncCreateExternalTable(runtime, ++t.TxId, "/MyRoot/DirExternalTable", R"(
                            Name: "external_table1"
                            DataSourcePath: "/MySource"
                            Columns { Name: "a" Type: "Int32" NotNull: true }
                            Columns { Name: "b" Type: "Int32" NotNull: true }
                        )");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});

            {
                TInactiveZone inactive(activeZone);
                auto describeResult =  DescribePath(runtime, "/MyRoot/DirExternalTable/external_table1");
                TestDescribeResult(describeResult, {NLs::Finished});

                UNIT_ASSERT(describeResult.GetPathDescription().HasExternalTableDescription());
                const auto& externalTableDescription = describeResult.GetPathDescription().GetExternalTableDescription();
                UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetName(), "external_table1");
                UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetDataSourcePath(), "/MySource");
                UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetVersion(), 1);
                auto& columns = externalTableDescription.GetColumns();
                UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetName(), "a");
                UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetType(), "Int32");
                UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetNotNull(), true);
                UNIT_ASSERT_VALUES_EQUAL(columns.Get(1).GetName(), "b");
                UNIT_ASSERT_VALUES_EQUAL(columns.Get(1).GetType(), "Int32");
                UNIT_ASSERT_VALUES_EQUAL(columns.Get(1).GetNotNull(), true);
            }
        });
    }

    Y_UNIT_TEST(ParallelCreateDrop) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncCreateExternalTable(runtime, ++t.TxId, "/MyRoot", R"(
                            Name: "DropMe"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                        )");
            AsyncDropExternalTable(runtime, ++t.TxId, "/MyRoot", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);


            TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DropMe"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(SimpleDropExternalTableWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"ExternalTable\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalTable"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(SimpleDropExternalTableWithReboots2) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot",
                                "Name: \"ExternalTable\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalTable"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(DropExternalTableWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalTable(runtime, t.TxId, "/MyRoot",
                                R"(Name: "ExternalTable"
                                          Columns { Name: "RowId"      Type: "Uint64"}
                                          Columns { Name: "Value"      Type: "Utf8"})");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalTable"),
                                   {NLs::PathNotExist});

                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot",
                                R"(Name: "ExternalTable"
                                          Columns { Name: "RowId"      Type: "Uint64"}
                                          Columns { Name: "Value"      Type: "Utf8"})");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(CreateDroppedExternalTableWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot",
                                R"(Name: "ExternalTable"
                                          Columns { Name: "RowId"      Type: "Uint64"}
                                          Columns { Name: "Value"      Type: "Utf8"})");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot",
                                R"(Name: "ExternalTable"
                                          Columns { Name: "RowId"      Type: "Uint64"}
                                          Columns { Name: "Value"      Type: "Utf8"})");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }
        });
    }

    Y_UNIT_TEST(CreateDroppedExternalTableAndDropWithReboots) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot",
                                R"(Name: "ExternalTable"
                                          Columns { Name: "RowId"      Type: "Uint64"}
                                          Columns { Name: "Value"      Type: "Utf8"})");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot",
                                R"(Name: "ExternalTable"
                                          Columns { Name: "RowId"      Type: "Uint64"}
                                          Columns { Name: "Value"      Type: "Utf8"})");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalTable"),
                                   {NLs::PathNotExist});
            }
        });
    }
}

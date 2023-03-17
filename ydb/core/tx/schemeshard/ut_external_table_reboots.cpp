#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

void CreateExternalDataSource(TTestActorRuntime& runtime, TTestEnv& env, ui64 txId) {
    AsyncCreateExternalDataSource(runtime, txId, "/MyRoot",R"(
            Name: "ExternalDataSource"
            SourceType: "ObjectStorage"
            Location: "https://s3.cloud.net/my_bucket"
            Auth {
                None {
                }
            }
        )");

    env.TestWaitNotification(runtime, txId);

    TestLs(runtime, "/MyRoot/ExternalDataSource", false, NLs::PathExist);
}

}

Y_UNIT_TEST_SUITE(TExternalTableTestReboots) {
    Y_UNIT_TEST(CreateExternalTableWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateExternalDataSource(runtime, *t.TestEnv, ++t.TxId);
            AsyncMkDir(runtime, ++t.TxId, "/MyRoot", "DirExternalTable");
            AsyncCreateExternalTable(runtime, ++t.TxId, "/MyRoot/DirExternalTable", R"(
                    Name: "ExternalTable"
                    SourceType: "General"
                    DataSourcePath: "/MyRoot/ExternalDataSource"
                    Location: "/"
                    Columns { Name: "a" Type: "Int32" NotNull: true }
                    Columns { Name: "b" Type: "Int32" NotNull: true }
                )");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-2, t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                auto describeResult =  DescribePath(runtime, "/MyRoot/DirExternalTable/ExternalTable");
                TestDescribeResult(describeResult, {NLs::Finished});

                UNIT_ASSERT(describeResult.GetPathDescription().HasExternalTableDescription());
                const auto& externalTableDescription = describeResult.GetPathDescription().GetExternalTableDescription();
                UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetName(), "ExternalTable");
                UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetDataSourcePath(), "/MyRoot/ExternalDataSource");
                UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetLocation(), "/");
                UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetSourceType(), "ObjectStorage");
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

    Y_UNIT_TEST(ParallelCreateDrop) {
        using ESts = NKikimrScheme::EStatus;
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateExternalDataSource(runtime, *t.TestEnv, t.TxId);
            AsyncCreateExternalTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "DropMe"
                    SourceType: "General"
                    DataSourcePath: "/MyRoot/ExternalDataSource"
                    Location: "/"
                    Columns { Name: "RowId" Type: "Uint64" }
                    Columns { Name: "Value" Type: "Utf8" }
                )");
            AsyncDropExternalTable(runtime, ++t.TxId, "/MyRoot", "DropMe");
            TestModificationResults(runtime, t.TxId - 1, {ESts::StatusAccepted});
            t.TestEnv->TestWaitNotification(runtime, t.TxId - 1);

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
            CreateExternalDataSource(runtime, *t.TestEnv, ++t.TxId);
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot",R"(
                        Name: "ExternalTable"
                        SourceType: "General"
                        DataSourcePath: "/MyRoot/ExternalDataSource"
                        Location: "/"
                        Columns { Name: "a" Type: "Int32" NotNull: true }
                        Columns { Name: "b" Type: "Int32" NotNull: true }
                    )");

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

    Y_UNIT_TEST(SimpleDropExternalTableWithReboots2) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateExternalDataSource(runtime, *t.TestEnv, ++t.TxId);
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot",R"(
                        Name: "ExternalTable"
                        SourceType: "General"
                        DataSourcePath: "/MyRoot/ExternalDataSource"
                        Location: "/"
                        Columns { Name: "a" Type: "Int32" NotNull: true }
                        Columns { Name: "b" Type: "Int32" NotNull: true }
                    )");
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
            CreateExternalDataSource(runtime, *t.TestEnv, ++t.TxId);
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalTable(runtime, t.TxId, "/MyRoot", R"(
                        Name: "ExternalTable"
                        SourceType: "General"
                        DataSourcePath: "/MyRoot/ExternalDataSource"
                        Location: "/"
                        Columns { Name: "RowId"      Type: "Uint64"}
                        Columns { Name: "Value"      Type: "Utf8"}
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalTable"),
                                   {NLs::PathNotExist});

                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "ExternalTable"
                        SourceType: "General"
                        DataSourcePath: "/MyRoot/ExternalDataSource"
                        Location: "/"
                        Columns { Name: "RowId"      Type: "Uint64"}
                        Columns { Name: "Value"      Type: "Utf8"}
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalTable"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(CreateDroppedExternalTableWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateExternalDataSource(runtime, *t.TestEnv, ++t.TxId);
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "ExternalTable"
                        SourceType: "General"
                        DataSourcePath: "/MyRoot/ExternalDataSource"
                        Location: "/"
                        Columns { Name: "RowId"      Type: "Uint64"}
                        Columns { Name: "Value"      Type: "Utf8"}
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "ExternalTable"
                    SourceType: "General"
                    DataSourcePath: "/MyRoot/ExternalDataSource"
                    Location: "/"
                    Columns { Name: "RowId"      Type: "Uint64"}
                    Columns { Name: "Value"      Type: "Utf8"}
                )");
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
            CreateExternalDataSource(runtime, *t.TestEnv, ++t.TxId);
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "ExternalTable"
                        SourceType: "General"
                        DataSourcePath: "/MyRoot/ExternalDataSource"
                        Location: "/"
                        Columns { Name: "RowId"      Type: "Uint64"}
                        Columns { Name: "Value"      Type: "Utf8"}
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropExternalTable(runtime, ++t.TxId, "/MyRoot", "ExternalTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateExternalTable(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "ExternalTable"
                        SourceType: "General"
                        DataSourcePath: "/MyRoot/ExternalDataSource"
                        Location: "/"
                        Columns { Name: "RowId"      Type: "Uint64"}
                        Columns { Name: "Value"      Type: "Utf8"}
                    )");
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

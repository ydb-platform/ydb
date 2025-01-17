#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

namespace {

void CreateExternalDataSource(TTestBasicRuntime& runtime, TTestEnv& env, ui64 txId) {
    TestCreateExternalDataSource(runtime, txId, "/MyRoot",R"(
            Name: "ExternalDataSource"
            SourceType: "ObjectStorage"
            Location: "https://s3.cloud.net/my_bucket"
            Auth {
                None {
                }
            }
        )", {NKikimrScheme::StatusAccepted});

    env.TestWaitNotification(runtime, txId);

    TestLs(runtime, "/MyRoot/ExternalDataSource", false, NLs::PathExist);
}

}

Y_UNIT_TEST_SUITE(TExternalTableTest) {
    Y_UNIT_TEST(CreateExternalTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        CreateExternalDataSource(runtime, env, txId++);
        TestCreateExternalTable(runtime, txId++, "/MyRoot", R"(
                Name: "ExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key" Type: "Uint64" }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId - 1);

        TestLs(runtime, "/MyRoot/ExternalTable", false, NLs::PathExist);
    }

    Y_UNIT_TEST(DropExternalTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        CreateExternalDataSource(runtime, env, txId++);
        TestCreateExternalTable(runtime, txId++, "/MyRoot", R"(
                Name: "ExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key" Type: "Uint64" }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId - 1);

        TestLs(runtime, "/MyRoot/ExternalTable", false, NLs::PathExist);

        TestDropExternalTable(runtime, ++txId, "/MyRoot", "ExternalTable");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/ExternalTable", false, NLs::PathNotExist);
    }

    using TRuntimeTxFn = std::function<void(TTestBasicRuntime&, ui64)>;

    void DropTwice(const TString& path, TRuntimeTxFn createFn, TRuntimeTxFn dropFn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        CreateExternalDataSource(runtime, env, txId++);
        createFn(runtime, ++txId);
        env.TestWaitNotification(runtime, txId);

        dropFn(runtime, ++txId);
        dropFn(runtime, ++txId);
        TestModificationResult(runtime, txId - 1);

        auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>();
        UNIT_ASSERT(ev);

        const auto& record = ev->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusMultipleModifications);
        UNIT_ASSERT_VALUES_EQUAL(record.GetPathDropTxId(), txId - 1);

        env.TestWaitNotification(runtime, txId - 1);
        TestDescribeResult(DescribePath(runtime, path), {
            NLs::PathNotExist
        });
    }

    Y_UNIT_TEST(DropTableTwice) {
        auto createFn = [](TTestBasicRuntime& runtime, ui64 txId) {
            TestCreateExternalTable(runtime, txId, "/MyRoot", R"(
                    Name: "ExternalTable"
                    SourceType: "General"
                    DataSourcePath: "/MyRoot/ExternalDataSource"
                    Location: "/"
                    Columns { Name: "key"   Type: "Uint64" }
                    Columns { Name: "value" Type: "Utf8" }
                )");
        };

        auto dropFn = [](TTestBasicRuntime& runtime, ui64 txId) {
            AsyncDropExternalTable(runtime, txId, "/MyRoot", "ExternalTable");
        };

        DropTwice("/MyRoot/ExternalTable", createFn, dropFn);
    }

    Y_UNIT_TEST(ParallelCreateExternalTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        CreateExternalDataSource(runtime, env, txId++);
        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "ExternalTable1"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "RowId" Type: "Uint64"}
                Columns { Name: "Value" Type: "Utf8"}
            )");
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "ExternalTable2"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key1"  Type: "Uint32"}
                Columns { Name: "key2"  Type: "Utf8"}
                Columns { Name: "RowId" Type: "Uint64"}
                Columns { Name: "Value" Type: "Utf8"}
            )");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        TestDescribe(runtime, "/MyRoot/DirA/ExternalTable1");
        TestDescribe(runtime, "/MyRoot/DirA/ExternalTable2");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(7)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/ExternalTable1"),
                           {NLs::PathVersionEqual(2)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/ExternalTable2"),
                           {NLs::PathVersionEqual(2)});
    }

    Y_UNIT_TEST(ParallelCreateSameExternalTable) {
        using ESts = NKikimrScheme::EStatus;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TString tableConfig = R"(
                Name: "NilNoviSubLuna"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key"   Type: "Uint64"}
                Columns { Name: "value" Type: "Uint64"}
            )";
        CreateExternalDataSource(runtime, env, txId++);
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot", tableConfig);
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot", tableConfig);
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot", tableConfig);

        ui64 sts[3];
        sts[0] = TestModificationResults(runtime, txId-2, {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});
        sts[1] = TestModificationResults(runtime, txId-1, {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});
        sts[2] = TestModificationResults(runtime, txId,   {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});

        for (ui32 i=0; i<3; ++i) {
            if (sts[i] == ESts::StatusAlreadyExists) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                                   {NLs::Finished,
                                    NLs::IsExternalTable});
            }

            if (sts[i] == ESts::StatusMultipleModifications) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                                   {NLs::Finished,
                                    NLs::IsExternalTable});
            }
        }

        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                           {NLs::Finished,
                            NLs::IsExternalTable,
                            NLs::PathVersionEqual(2)});

        TestCreateExternalTable(runtime, ++txId, "/MyRoot", tableConfig, {ESts::StatusAlreadyExists});

    }


    Y_UNIT_TEST(ReadOnlyMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        CreateExternalDataSource(runtime, env, txId++);
        AsyncMkDir(runtime, ++txId, "/MyRoot", "SubDirA");
        AsyncCreateExternalTable(runtime, ++txId, "/MyRoot", R"(
                Name: "ExternalTable1"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "RowId" Type: "Uint64"}
                Columns { Name: "Value" Type: "Utf8"}
            )");
        // Set ReadOnly
        SetSchemeshardReadOnlyMode(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Verify that table creation successfully finished
        env.TestWaitNotification(runtime, txId);

        // Check that describe works
        TestDescribeResult(DescribePath(runtime, "/MyRoot/SubDirA"),
                           {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalTable1"),
                           {NLs::Finished,
                            NLs::IsExternalTable});

        // Check that new modifications fail
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirBBBB", {NKikimrScheme::StatusReadOnly});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot", R"(
                Name: "ExternalTable1"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "RowId" Type: "Uint64"}
                Columns { Name: "Value" Type: "Utf8"}
            )", {NKikimrScheme::StatusReadOnly});

        // Disable ReadOnly
        SetSchemeshardReadOnlyMode(runtime, false);
        sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Check that modifications now work again
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirBBBB");
    }

    Y_UNIT_TEST(SchemeErrors) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);
        CreateExternalDataSource(runtime, env, txId++);
        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "Table2"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "RowId" Type: "BlaBlaType"}
            )", {{NKikimrScheme::StatusSchemeError, "Type 'BlaBlaType' specified for column 'RowId' is not supported"}});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "Table2"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "" Type: "Uint64"}
            )", {{NKikimrScheme::StatusSchemeError, "Columns cannot have an empty name"}});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "Table2"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "RowId" TypeId: 27}
            )", {{NKikimrScheme::StatusSchemeError, "a"}});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "Table2"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "RowId" }
            )", {{NKikimrScheme::StatusSchemeError, "Missing Type for column 'RowId'"}});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "Table2"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "RowId" Type: "Uint64" Id: 2}
                Columns { Name: "RowId2" Type: "Uint64" Id: 2 }
            )", {{NKikimrScheme::StatusSchemeError, "Duplicate column id: 2"}});
        TestCreateExternalTable(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "Table2"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource1"
                Location: "/"
                Columns { Name: "RowId" Type: "Uint64"}
                Columns { Name: "Value" Type: "Utf8"}
            )", {{NKikimrScheme::StatusPathDoesNotExist, "Check failed: path: '/MyRoot/ExternalDataSource1'"}});
    }

    Y_UNIT_TEST(ReplaceExternalTableIfNotExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableReplaceIfExistsForExternalEntities(true));
        ui64 txId = 100;

        CreateExternalDataSource(runtime, env, ++txId);
        TestCreateExternalTable(runtime, ++txId, "/MyRoot", R"(
                Name: "ExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key" Type: "Uint64" }
                ReplaceIfExists: true
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        {
            auto describeResult = DescribePath(runtime, "/MyRoot/ExternalTable");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasExternalTableDescription());
            const auto& externalTableDescription = describeResult.GetPathDescription().GetExternalTableDescription();
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetName(), "ExternalTable");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetDataSourcePath(), "/MyRoot/ExternalDataSource");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetLocation(), "/");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetVersion(), 1);
            auto& columns = externalTableDescription.GetColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetName(), "key");
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetType(), "Uint64");
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetNotNull(), false);
        }

        TestCreateExternalTable(runtime, ++txId, "/MyRoot", R"(
                Name: "ExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/new_location"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Uint64" }
                ReplaceIfExists: true
            )", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        {
            auto describeResult = DescribePath(runtime, "/MyRoot/ExternalTable");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasExternalTableDescription());
            const auto& externalTableDescription = describeResult.GetPathDescription().GetExternalTableDescription();
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetName(), "ExternalTable");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetDataSourcePath(), "/MyRoot/ExternalDataSource");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetLocation(), "/new_location");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetVersion(), 2);
            auto& columns = externalTableDescription.GetColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetName(), "key");
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetType(), "Uint64");
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetNotNull(), false);
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(1).GetName(), "value");
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(1).GetType(), "Uint64");
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(1).GetNotNull(), false);
        }
    }

    Y_UNIT_TEST(CreateExternalTableShouldFailIfSuchEntityAlreadyExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableReplaceIfExistsForExternalEntities(true));
        ui64 txId = 100;

        CreateExternalDataSource(runtime, env, ++txId);
        TestCreateExternalTable(runtime, ++txId, "/MyRoot", R"(
                Name: "ExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key" Type: "Uint64" }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        {
            auto describeResult = DescribePath(runtime, "/MyRoot/ExternalTable");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasExternalTableDescription());
            const auto& externalTableDescription = describeResult.GetPathDescription().GetExternalTableDescription();
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetName(), "ExternalTable");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetDataSourcePath(), "/MyRoot/ExternalDataSource");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetLocation(), "/");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetVersion(), 1);
            auto& columns = externalTableDescription.GetColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetName(), "key");
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetType(), "Uint64");
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetNotNull(), false);
        }

        TestCreateExternalTable(runtime, ++txId, "/MyRoot", R"(
                Name: "ExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/new_location"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Uint64" }
            )", {NKikimrScheme::StatusAlreadyExists});
        env.TestWaitNotification(runtime, txId);

        {
            auto describeResult = DescribePath(runtime, "/MyRoot/ExternalTable");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasExternalTableDescription());
            const auto& externalTableDescription = describeResult.GetPathDescription().GetExternalTableDescription();
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetName(), "ExternalTable");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetDataSourcePath(), "/MyRoot/ExternalDataSource");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetLocation(), "/");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalTableDescription.GetVersion(), 1);
            auto& columns = externalTableDescription.GetColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetName(), "key");
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetType(), "Uint64");
            UNIT_ASSERT_VALUES_EQUAL(columns.Get(0).GetNotNull(), false);
        }
    }

    Y_UNIT_TEST(ReplaceExternalTableShouldFailIfEntityOfAnotherTypeWithSameNameExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableReplaceIfExistsForExternalEntities(true));
        ui64 txId = 100;

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
                Name: "UniqueName"
                QueryText: "Some query"
            )", {NKikimrScheme::StatusAccepted}
        );
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/UniqueName", false, NLs::PathExist);

        CreateExternalDataSource(runtime, env, ++txId);
        TestCreateExternalTable(runtime, ++txId, "/MyRoot", R"(
                Name: "UniqueName"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key" Type: "Uint64" }
                ReplaceIfExists: true
            )", {{NKikimrScheme::StatusNameConflict, "error: unexpected path type"}});

        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(ReplaceExternalTableIfNotExistsShouldFailIfFeatureFlagIsNotSet) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableReplaceIfExistsForExternalEntities(false));
        ui64 txId = 100;

        CreateExternalDataSource(runtime, env, ++txId);
        TestCreateExternalTable(runtime, ++txId, "/MyRoot", R"(
                Name: "ExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key" Type: "Uint64" }
                ReplaceIfExists: true
            )", {{NKikimrScheme::StatusPreconditionFailed, "Unsupported: feature flag EnableReplaceIfExistsForExternalEntities is off"}});

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/ExternalTable", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(Decimal) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableParameterizedDecimal(true));
        ui64 txId = 100;
        CreateExternalDataSource(runtime, env, txId++);
        TestCreateExternalTable(runtime, txId++, "/MyRoot", R"_(
                Name: "ExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key" Type: "Decimal(35,9)" }
            )_", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId - 1);

        TestLs(runtime, "/MyRoot/ExternalTable", false, NLs::PathExist);
    }    
}

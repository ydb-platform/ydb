#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TExternalDataSourceTest) {
    Y_UNIT_TEST(CreateExternalDataSource) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExternalDataSource(runtime, txId++, "/MyRoot",R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, 100);

        TestLs(runtime, "/MyRoot/MyExternalDataSource", false, NLs::PathExist);
    }

    Y_UNIT_TEST(CreateExternalDataSourceWithProperties) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExternalDataSource(runtime, txId++, "/MyRoot",R"(
                Name: "MyExternalDataSource"
                SourceType: "PostgreSQL"
                Location: "localhost:5432"
                Auth {
                    Basic {
                        Login: "my_login",
                        PasswordSecretName: "password_secret"
                    }
                }
                Properties {
                    Properties {
                        key: "mdb_cluster_id",
                        value: "id"
                    }
                    Properties {
                        key: "database_name",
                        value: "postgres"
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, 100);

        TestLs(runtime, "/MyRoot/MyExternalDataSource", false, NLs::PathExist);
    }

    Y_UNIT_TEST(DropExternalDataSource) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExternalDataSource(runtime, txId++, "/MyRoot",R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )",{NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, 100);

        TestLs(runtime, "/MyRoot/MyExternalDataSource", false, NLs::PathExist);

        TestDropExternalDataSource(runtime, ++txId, "/MyRoot", "MyExternalDataSource");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyExternalDataSource", false, NLs::PathNotExist);
    }

    using TRuntimeTxFn = std::function<void(TTestBasicRuntime&, ui64)>;

    void DropTwice(const TString& path, TRuntimeTxFn createFn, TRuntimeTxFn dropFn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

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
            TestCreateExternalDataSource(runtime, txId, "/MyRoot", R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )");
        };

        auto dropFn = [](TTestBasicRuntime& runtime, ui64 txId) {
            AsyncDropExternalDataSource(runtime, txId, "/MyRoot", "MyExternalDataSource");
        };

        DropTwice("/MyRoot/MyExternalDataSource", createFn, dropFn);
    }

    Y_UNIT_TEST(ParallelCreateExternalDataSource) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncCreateExternalDataSource(runtime, ++txId, "/MyRoot/DirA",R"(
                Name: "MyExternalDataSource1"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )");
        AsyncCreateExternalDataSource(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "MyExternalDataSource2"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        TestDescribe(runtime, "/MyRoot/DirA/MyExternalDataSource1");
        TestDescribe(runtime, "/MyRoot/DirA/MyExternalDataSource2");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(7)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/MyExternalDataSource1"),
                           {NLs::PathVersionEqual(2)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/MyExternalDataSource2"),
                           {NLs::PathVersionEqual(2)});
    }

    Y_UNIT_TEST(ParallelCreateSameExternalDataSource) {
        using ESts = NKikimrScheme::EStatus;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TString dataSourceConfig = R"(
                Name: "NilNoviSubLuna"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )";

        AsyncCreateExternalDataSource(runtime, ++txId, "/MyRoot", dataSourceConfig);
        AsyncCreateExternalDataSource(runtime, ++txId, "/MyRoot", dataSourceConfig);
        AsyncCreateExternalDataSource(runtime, ++txId, "/MyRoot", dataSourceConfig);

        ui64 sts[3];
        sts[0] = TestModificationResults(runtime, txId-2, {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});
        sts[1] = TestModificationResults(runtime, txId-1, {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});
        sts[2] = TestModificationResults(runtime, txId,   {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});

        for (ui32 i=0; i<3; ++i) {
            if (sts[i] == ESts::StatusAlreadyExists) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                                   {NLs::Finished,
                                    NLs::IsExternalDataSource});
            }

            if (sts[i] == ESts::StatusMultipleModifications) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                                   {NLs::Finished,
                                    NLs::IsExternalDataSource});
            }
        }

        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                           {NLs::Finished,
                            NLs::IsExternalDataSource,
                            NLs::PathVersionEqual(2)});

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot", dataSourceConfig, {ESts::StatusAlreadyExists});

    }


    Y_UNIT_TEST(ReadOnlyMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "SubDirA");
        AsyncCreateExternalDataSource(runtime, ++txId, "/MyRoot",R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
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
        TestDescribeResult(DescribePath(runtime, "/MyRoot/MyExternalDataSource"),
                           {NLs::Finished,
                            NLs::IsExternalDataSource});

        // Check that new modifications fail
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirBBBB", {NKikimrScheme::StatusReadOnly});
        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot",R"(
                Name: "MyExternalDataSource2"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
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

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot/DirA",R"(
                Name: "MyExternalDataSource"
                SourceType: "DataStream"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )", {{NKikimrScheme::StatusSchemeError, "External source with type DataStream was not found"}});
        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot/DirA",R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
            )", {{NKikimrScheme::StatusSchemeError, "Authorization method isn't specified"}});
        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot/DirA", Sprintf(R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "%s"
                Auth {
                    None {
                    }
                }
            )", TString{1001, 'a'}.c_str()), {{NKikimrScheme::StatusSchemeError, "Maximum length of location must be less or equal equal to 1000 but got 1001"}});
        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot/DirA", Sprintf(R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Installation: "%s"
                Auth {
                    None {
                    }
                }
            )", TString{1001, 'a'}.c_str()), {{NKikimrScheme::StatusSchemeError, "Maximum length of installation must be less or equal equal to 1000 but got 1001"}});
        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot/DirA",R"(
                Name: ""
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )", {{NKikimrScheme::StatusSchemeError, "error: path part shouldn't be empty"}});
    }

    Y_UNIT_TEST(PreventDeletionOfDependentDataSources) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExternalDataSource(runtime, txId++, "/MyRoot",R"(
                Name: "ExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, 100);

        TestLs(runtime, "/MyRoot/ExternalDataSource", false, NLs::PathExist);

        TestCreateExternalTable(runtime, txId++, "/MyRoot", R"(
                Name: "ExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key" Type: "Uint64" }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId - 1);

        TestLs(runtime, "/MyRoot/ExternalTable", false, NLs::PathExist);


        TestDropExternalDataSource(runtime, ++txId, "/MyRoot", "ExternalDataSource",
                {{NKikimrScheme::StatusSchemeError, "Other entities depend on this data source, please remove them at the beginning: /MyRoot/ExternalTable"}});
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/ExternalDataSource", false, NLs::PathExist);
    }

    Y_UNIT_TEST(RemovingReferencesFromDataSources) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExternalDataSource(runtime, txId++, "/MyRoot",R"(
                Name: "ExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, 100);

        TestLs(runtime, "/MyRoot/ExternalDataSource", false, NLs::PathExist);

        TestCreateExternalTable(runtime, txId++, "/MyRoot", R"(
                Name: "ExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "/"
                Columns { Name: "key" Type: "Uint64" }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId - 1);

        TestLs(runtime, "/MyRoot/ExternalTable", false, NLs::PathExist);

        TestDropExternalTable(runtime, ++txId, "/MyRoot", "ExternalTable",
            {NKikimrScheme::StatusAccepted});

        TestLs(runtime, "/MyRoot/ExternalTable", false, NLs::PathNotExist);

        TestDropExternalDataSource(runtime, ++txId, "/MyRoot", "ExternalDataSource",
                {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/ExternalDataSource", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(ReplaceExternalDataSourceIfNotExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableReplaceIfExistsForExternalEntities(true));
        ui64 txId = 100;

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot",R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
                ReplaceIfExists: true
            )",{NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        {
            auto describeResult =  DescribePath(runtime, "/MyRoot/MyExternalDataSource");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasExternalDataSourceDescription());
            const auto& externalDataSourceDescription = describeResult.GetPathDescription().GetExternalDataSourceDescription();
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetName(), "MyExternalDataSource");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetVersion(), 1);
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetLocation(), "https://s3.cloud.net/my_bucket");
            UNIT_ASSERT_EQUAL(externalDataSourceDescription.GetAuth().identity_case(), NKikimrSchemeOp::TAuth::kNone);
        }

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot",R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_new_bucket"
                Auth {
                    None {
                    }
                }
                ReplaceIfExists: true
            )",{NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        {
            auto describeResult =  DescribePath(runtime, "/MyRoot/MyExternalDataSource");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasExternalDataSourceDescription());
            const auto& externalDataSourceDescription = describeResult.GetPathDescription().GetExternalDataSourceDescription();
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetName(), "MyExternalDataSource");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetVersion(), 2);
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetLocation(), "https://s3.cloud.net/my_new_bucket");
            UNIT_ASSERT_EQUAL(externalDataSourceDescription.GetAuth().identity_case(), NKikimrSchemeOp::TAuth::kNone);
        }
    }

    Y_UNIT_TEST(CreateExternalDataSourceShouldFailIfSuchEntityAlreadyExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableReplaceIfExistsForExternalEntities(true));
        ui64 txId = 100;

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot",R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
            )",{NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        {
            auto describeResult =  DescribePath(runtime, "/MyRoot/MyExternalDataSource");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasExternalDataSourceDescription());
            const auto& externalDataSourceDescription = describeResult.GetPathDescription().GetExternalDataSourceDescription();
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetName(), "MyExternalDataSource");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetVersion(), 1);
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetLocation(), "https://s3.cloud.net/my_bucket");
            UNIT_ASSERT_EQUAL(externalDataSourceDescription.GetAuth().identity_case(), NKikimrSchemeOp::TAuth::kNone);
        }

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot",R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_new_bucket"
                Auth {
                    None {
                    }
                }
            )",{NKikimrScheme::StatusAlreadyExists});
        env.TestWaitNotification(runtime, txId);

        {
            auto describeResult =  DescribePath(runtime, "/MyRoot/MyExternalDataSource");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasExternalDataSourceDescription());
            const auto& externalDataSourceDescription = describeResult.GetPathDescription().GetExternalDataSourceDescription();
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetName(), "MyExternalDataSource");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetVersion(), 1);
            UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetLocation(), "https://s3.cloud.net/my_bucket");
            UNIT_ASSERT_EQUAL(externalDataSourceDescription.GetAuth().identity_case(), NKikimrSchemeOp::TAuth::kNone);
        }
    }

    Y_UNIT_TEST(ReplaceExternalDataStoreShouldFailIfEntityOfAnotherTypeWithSameNameExists) {
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

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot",R"(
                Name: "UniqueName"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
                ReplaceIfExists: true
            )",{{NKikimrScheme::StatusNameConflict, "error: unexpected path type"}});

        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(ReplaceExternalDataSourceIfNotExistsShouldFailIfFeatureFlagIsNotSet) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableReplaceIfExistsForExternalEntities(false));
        ui64 txId = 100;

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot",R"(
                Name: "MyExternalDataSource"
                SourceType: "ObjectStorage"
                Location: "https://s3.cloud.net/my_bucket"
                Auth {
                    None {
                    }
                }
                ReplaceIfExists: true
            )",{{NKikimrScheme::StatusPreconditionFailed, "Unsupported: feature flag EnableReplaceIfExistsForExternalEntities is off"}});

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyExternalDataSource", false, NLs::PathNotExist);
    }
}

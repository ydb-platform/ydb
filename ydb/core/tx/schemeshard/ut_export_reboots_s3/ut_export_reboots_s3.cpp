#include <ydb/core/tx/schemeshard/ut_helpers/export_reboots_common.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/util/aws.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <library/cpp/testing/hook/hook.h>

#include <util/folder/tempdir.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

using namespace NKikimrSchemeOp;
using namespace NKikimr::NWrappers::NTestHelpers;
using namespace NSchemeShardUT_Private;
using namespace NSchemeShardUT_Private::NExportReboots;

namespace {

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    NKikimr::InitAwsAPI();
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    NKikimr::ShutdownAwsAPI();
}

}

Y_UNIT_TEST_SUITE(TExportToS3WithRebootsTests) {

    struct TExportItem {
        TString SourcePath;
        TString Destination;
    };

    TString MakeS3RequestTemplate(const TVector<TExportItem>& items,
                                  const TString& extraSettings = "")
    {
        TStringBuilder sb;
        sb << "ExportToS3Settings { endpoint: \"localhost:%d\" scheme: HTTP ";
        if (extraSettings) sb << extraSettings << " ";
        for (const auto& item : items) {
            sb << "items { source_path: \"" << item.SourcePath
               << "\" destination_prefix: \"" << item.Destination << "\" } ";
        }
        sb << "}";
        return sb;
    }

    TString MakeFsRequestTemplate(const TVector<TExportItem>& items,
                                  const TString& extraSettings = "")
    {
        TStringBuilder sb;
        sb << "ExportToFsSettings { base_path: \"%s\" ";
        if (extraSettings) sb << extraSettings << " ";
        for (const auto& item : items) {
            sb << "items { source_path: \"" << item.SourcePath
               << "\" destination_path: \"" << item.Destination << "\" } ";
        }
        sb << "}";
        return sb;
    }

    using TRunFnWithSetup = void(*)(const TVector<TTypedScheme>&, const TString&, TTestWithReboots&, TRuntimeSetup);

    void Decorate(TTestWithReboots& t, bool isFs,
        const TVector<TTypedScheme>& schemeObjects,
        const TVector<TExportItem>& items,
        TRunFnWithSetup func, const TTestEnvOptions& opts,
        const TString& extraSettings = "")
    {
        t.GetTestEnvOptions() = opts;

        if (isFs) {
            TTempDir tempDir;
            TString request = Sprintf(
                MakeFsRequestTemplate(items, extraSettings).c_str(),
                tempDir.Path().c_str());
            func(schemeObjects, request, t, [](TTestActorRuntime& runtime) {
                runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
            });
        } else {
            TPortManager portManager;
            const ui16 port = portManager.GetPort();
            TS3Mock s3Mock({}, TS3Mock::TSettings(port));
            UNIT_ASSERT(s3Mock.Start());
            TString request = Sprintf(
                MakeS3RequestTemplate(items, extraSettings).c_str(), port);
            func(schemeObjects, request, t, {});
        }
    }

    template <bool IsFs>
    void RunExport(TTestWithReboots& t,
        const TVector<TTypedScheme>& schemeObjects,
        const TVector<TExportItem>& items,
        const TTestEnvOptions& opts = TTestWithReboots::GetDefaultTestEnvOptions(),
        const TString& extraSettings = "")
    {
        Decorate(t, IsFs, schemeObjects, items, &Run, opts, extraSettings);
    }

    template <bool IsFs>
    void CancelExport(TTestWithReboots& t,
        const TVector<TTypedScheme>& schemeObjects,
        const TVector<TExportItem>& items,
        const TTestEnvOptions& opts = TTestWithReboots::GetDefaultTestEnvOptions(),
        const TString& extraSettings = "")
    {
        Decorate(t, IsFs, schemeObjects, items, &Cancel, opts, extraSettings);
    }

    template <bool IsFs>
    void ForgetExport(TTestWithReboots& t,
        const TVector<TTypedScheme>& schemeObjects,
        const TVector<TExportItem>& items,
        const TTestEnvOptions& opts = TTestWithReboots::GetDefaultTestEnvOptions(),
        const TString& extraSettings = "")
    {
        Decorate(t, IsFs, schemeObjects, items, &Forget, opts, extraSettings);
    }

    using TRunFn = void(*)(const TVector<TTypedScheme>&, const TString&, TTestWithReboots&);

    void DecorateS3(TTestWithReboots& t, const TVector<TTypedScheme>& schemeObjects, const TString& request,
        TRunFn func, const TTestEnvOptions& opts)
    {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        t.GetTestEnvOptions() = opts;
        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        func(schemeObjects, Sprintf(request.c_str(), port), t);
    }

    void RunS3(TTestWithReboots& t, const TVector<TTypedScheme>& schemeObjects, const TString& request,
        const TTestEnvOptions& opts = TTestWithReboots::GetDefaultTestEnvOptions())
    {
        DecorateS3(t, schemeObjects, request, &Run, opts);
    }

    // Simple tests -- parameterized with IsFs

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnSingleShardTable, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, {{"/MyRoot/Table", ""}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnMultiShardTable, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
        }, {{"/MyRoot/Table", ""}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleTable, 2, 1, false) {
        // same as ShouldSucceedOnSingleShardTable
        Y_UNUSED(t);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnManyTables, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
            R"(
                Name: "Table2"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, {{"/MyRoot/Table1", "table1"}, {"/MyRoot/Table2", "table2"}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnSingleView, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            {
                EPathTypeView,
                R"(
                    Name: "View"
                    QueryText: "some query"
                )"
            }
        }, {{"/MyRoot/View", ""}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnViewsAndTables, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            {
                EPathTypeView,
                R"(
                    Name: "View"
                    QueryText: "some query"
                )"
            }, {
                EPathTypeTable,
                R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Utf8" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )"
            }
        }, {{"/MyRoot/View", "view"}, {"/MyRoot/Table", "table"}});
    }

    // Complex S3-only test: checks s3Mock.GetData()
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnViewsAndTablesPermissions, 2, 1, false) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        t.GetTestEnvOptions() = TTestEnvOptions().EnablePermissionsExport(true);
        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableViewExport(true);
            runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
            {
                TInactiveZone inactive(activeZone);
                CreateSchemeObjects(t, runtime, {
                    {
                        EPathTypeView,
                        R"(
                            Name: "View"
                            QueryText: "some query"
                        )"
                    }, {
                        EPathTypeTable,
                        R"(
                            Name: "Table"
                            Columns { Name: "key" Type: "Utf8" }
                            Columns { Name: "value" Type: "Utf8" }
                            KeyColumnNames: ["key"]
                        )"
                    }
                });

                TestExport(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                    ExportToS3Settings {
                        endpoint: "localhost:%d"
                        scheme: HTTP
                        items {
                            source_path: "/MyRoot/View"
                            destination_prefix: "view"
                        }
                        items {
                            source_path: "/MyRoot/Table"
                            destination_prefix: "table"
                        }
                    }
                )", port));
            }

            const ui64 exportId = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, exportId);

            {
                TInactiveZone inactive(activeZone);
                TestGetExport(runtime, exportId, "/MyRoot");
            }
        });

        auto* tablePermissions = s3Mock.GetData().FindPtr("/table/permissions.pb");
        UNIT_ASSERT(tablePermissions);

        auto* viewPermissions = s3Mock.GetData().FindPtr("/view/permissions.pb");
        UNIT_ASSERT(viewPermissions);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelShouldSucceedOnSingleShardTable, 2, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, {{"/MyRoot/Table", ""}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelShouldSucceedOnMultiShardTable, 2, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
        }, {{"/MyRoot/Table", ""}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnSingleTable, 2, 1, false) {
        // same as CancelShouldSucceedOnSingleShardTable
        Y_UNUSED(t);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelShouldSucceedOnManyTables, 2, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
            R"(
                Name: "Table2"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, {{"/MyRoot/Table1", "table1"}, {"/MyRoot/Table2", "table2"}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelShouldSucceedOnSingleView, 2, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            {
                EPathTypeView,
                R"(
                    Name: "View"
                    QueryText: "some query"
                )"
            }
        }, {{"/MyRoot/View", ""}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelShouldSucceedOnViewsAndTables, 4, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            {
                EPathTypeView,
                R"(
                    Name: "View"
                    QueryText: "some query"
                )"
            }, {
                EPathTypeTable,
                R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Utf8" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )"
            }
        }, {{"/MyRoot/View", "view"}, {"/MyRoot/Table", "table"}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnSingleShardTable, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, {{"/MyRoot/Table", ""}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnMultiShardTable, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
        }, {{"/MyRoot/Table", ""}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleTable, 2, 1, false) {
        // same as ForgetShouldSucceedOnSingleShardTable
        Y_UNUSED(t);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnManyTables, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
            R"(
                Name: "Table2"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, {{"/MyRoot/Table1", "table1"}, {"/MyRoot/Table2", "table2"}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnSingleView, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            {
                EPathTypeView,
                R"(
                    Name: "View"
                    QueryText: "some query"
                )"
            }
        }, {{"/MyRoot/View", ""}});
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnViewsAndTables, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            {
                EPathTypeView,
                R"(
                    Name: "View"
                    QueryText: "some query"
                )"
            }, {
                EPathTypeTable,
                R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Utf8" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )"
            }
        }, {{"/MyRoot/View", "view"}, {"/MyRoot/Table", "table"}});
    }

    class TTestData {
    public:
        static const TTypedScheme& Table() {
            return TableScheme;
        }

        static const TTypedScheme& IndexedTable() {
            return IndexedTableScheme;
        }

        static const TTypedScheme& Changefeed() {
            return ChangefeedScheme;
        }

        static const TTypedScheme& Topic() {
            return TopicScheme;
        }

        static const TTypedScheme& Replication() {
            return ReplicationScheme;
        }

        static const TTypedScheme& Transfer() {
            return TransferScheme;
        }

        static const TTypedScheme& ExternalDataSource() {
            return ExternalDataSourceScheme;
        }

        static const TTypedScheme& ExternalTable() {
            return ExternalTableScheme;
        }

        static TVector<TExportItem> Items(EPathType pathType = EPathType::EPathTypeTable) {
            switch (pathType) {
            case EPathType::EPathTypeTable:
                return {{"/MyRoot/Table", ""}};
            case EPathType::EPathTypeReplication:
                return {{"/MyRoot/Replication", ""}};
            case EPathType::EPathTypeTransfer:
                return {{"/MyRoot/Transfer", ""}};
            case EPathType::EPathTypeExternalDataSource:
                return {{"/MyRoot/DataSource", ""}};
            case EPathType::EPathTypeExternalTable:
                return {{"/MyRoot/ExternalTable", ""}};
            default:
                Y_ABORT("not supported");
            }
        }

        // Legacy S3-only request strings for complex tests
        static TString S3Request(EPathType pathType = EPathType::EPathTypeTable) {
            return MakeS3RequestTemplate(Items(pathType));
        }

    private:
        static const char* TableName;
        static const TTypedScheme TableScheme;
        static const TTypedScheme ChangefeedScheme;
        static const TTypedScheme TopicScheme;
        static const TTypedScheme ReplicationScheme;
        static const TTypedScheme TransferScheme;
        static const TTypedScheme ExternalDataSourceScheme;
        static const TTypedScheme ExternalTableScheme;
        static const TTypedScheme IndexedTableScheme;
    };

    const char* TTestData::TableName = "Table";

    const TTypedScheme TTestData::TableScheme = TTypedScheme {
        EPathTypeTable,
        Sprintf(R"(
            Name: "%s"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", TableName)
    };

    const TTypedScheme TTestData::ChangefeedScheme = TTypedScheme {
        EPathTypeCdcStream,
        Sprintf(R"(
            TableName: "%s"
            StreamDescription {
                Name: "update_feed"
                Mode: ECdcStreamModeUpdate
                Format: ECdcStreamFormatJson
                State: ECdcStreamStateReady
            }
        )", TableName)
    };

    const TTypedScheme TTestData::ReplicationScheme = TTypedScheme {
        EPathTypeReplication,
        R"(
            Name: "Replication"
            Config {
                SrcConnectionParams {
                    StaticCredentials {
                        User: "user"
                        Password: "pwd"
                    }
                }
                Specific {
                    Targets {
                        SrcPath: "/MyRoot/Table1"
                        DstPath: "/MyRoot/Table1Replica"
                    }
                    Targets {
                        SrcPath: "/MyRoot/Table2"
                        DstPath: "/MyRoot/Table2Replica"
                    }
                }
            }
        )"
    };

    const TTypedScheme TTestData::TransferScheme = TTypedScheme {
        EPathTypeTransfer,
        R"(
            Name: "Transfer"
            Config {
                TransferSpecific {
                    Target {
                        SrcPath: "/MyRoot/Topic"
                        DstPath: "/MyRoot/Table"
                        TransformLambda: "PRAGMA OrderedColumns;$transformation_lambda = ($msg) -> { return [ <| partition: $msg._partition, offset: $msg._offset, message: CAST($msg._data AS Utf8) |> ]; };$__ydb_transfer_lambda = $transformation_lambda;"
                        ConsumerName: "consumerName"
                    }
                }
            }
        )"
    };

    const TTypedScheme TTestData::ExternalDataSourceScheme = TTypedScheme {
        EPathTypeExternalDataSource,
        R"(
            Name: "DataSource"
            SourceType: "ObjectStorage"
            Location: "https://s3.cloud.net/bucket"
            Auth {
                Aws {
                    AwsAccessKeyIdSecretName: "id_secret",
                    AwsSecretAccessKeySecretName: "access_secret"
                    AwsRegion: "ru-central-1"
                }
            }
        )"
    };

    const TTypedScheme TTestData::ExternalTableScheme = TTypedScheme {
        EPathTypeExternalTable,
        R"(
            Name: "ExternalTable"
            SourceType: "General"
            DataSourcePath: "/MyRoot/DataSource"
            Location: "bucket"
            Columns { Name: "key" Type: "Uint64" NotNull: true }
            Columns { Name: "value1" Type: "Uint64" }
            Columns { Name: "value2" Type: "Utf8" NotNull: true }
        )"
    };

    const TTypedScheme TTestData::IndexedTableScheme = TTypedScheme {
        EPathTypeTableIndex, // TODO: Replace with IndexedTable
        Sprintf(R"(
            TableDescription {
                %s
            }
            IndexDescription {
                Name: "ByValue"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobalUnique
            }
        )", TableScheme.Scheme.c_str())
    };

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnSingleShardTableWithChangefeed, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            TTestData::Table(),
            TTestData::Changefeed()
        }, TTestData::Items());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelOnSingleShardTableWithChangefeed, 2, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            TTestData::Table(),
            TTestData::Changefeed()
        }, TTestData::Items());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnSingleShardTableWithChangefeed, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            TTestData::Table(),
            TTestData::Changefeed()
        }, TTestData::Items());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnSingleShardTableWithUniqueIndex, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            TTestData::IndexedTable()
        }, TTestData::Items());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnSingleShardTableWithUniqueIndex, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            TTestData::IndexedTable()
        }, TTestData::Items());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelShouldSucceedOnSingleShardTableWithUniqueIndex, 2, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            TTestData::IndexedTable()
        }, TTestData::Items());
    }

    // Complex S3-only tests that use TS3Mock directly
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedAutoDropping, 2, 1, false) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
            runtime.GetAppData().FeatureFlags.SetEnableExportAutoDropping(true);
            {
                TInactiveZone inactive(activeZone);
                CreateSchemeObjects(t, runtime, {
                    TTestData::Table()
                });

                TestExport(runtime, ++t.TxId, "/MyRoot", Sprintf(TTestData::S3Request().data(), port));
            }

            const ui64 exportId = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, exportId);

            {
                TInactiveZone inactive(activeZone);
                TestGetExport(runtime, exportId, "/MyRoot");
                TestRmDir(runtime, ++t.TxId, "/MyRoot", "DirA");
                auto desc = DescribePath(runtime, "/MyRoot");
                UNIT_ASSERT_EQUAL(desc.GetPathDescription().ChildrenSize(), 2);
                UNIT_ASSERT_EQUAL(desc.GetPathDescription().GetChildren(1).GetName(), "Table");
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldDisableAutoDropping, 2, 1, false) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
            runtime.GetAppData().FeatureFlags.SetEnableExportAutoDropping(false);
            {
                TInactiveZone inactive(activeZone);
                CreateSchemeObjects(t, runtime, {
                    TTestData::Table()
                });

                TestExport(runtime, ++t.TxId, "/MyRoot", Sprintf(TTestData::S3Request().data(), port));
            }

            const ui64 exportId = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, exportId);

            {
                TInactiveZone inactive(activeZone);
                TestGetExport(runtime, exportId, "/MyRoot");
                TestRmDir(runtime, ++t.TxId, "/MyRoot", "DirA");
                auto desc = DescribePath(runtime, "/MyRoot");
                UNIT_ASSERT_EQUAL(desc.GetPathDescription().ChildrenSize(), 3);
                const auto namesVector = {desc.GetPathDescription().GetChildren(1).GetName(),
                                          desc.GetPathDescription().GetChildren(2).GetName()};
                UNIT_ASSERT(IsIn(namesVector, "Table"));
                UNIT_ASSERT(IsIn(namesVector, "export-1003"));
            }
        });
    }

    // Topic tests use S3-specific TSimpleTopic::GetExportRequest(), keep S3-only
    using S3Func = void (*)(TTestWithReboots&, const TVector<TTypedScheme>&, const TString&, const TTestEnvOptions&);

    void TestSingleTopic(TTestWithReboots& t, S3Func func) {
        auto topic = NDescUT::TSimpleTopic(0, 2);
        func(t,
            {
                {
                    EPathTypePersQueueGroup,
                    topic.GetPrivateProto().DebugString()
                }
            }
            , topic.GetExportRequest()
            , TTestWithReboots::GetDefaultTestEnvOptions());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleTopic, 2, 1, false) {
        TestSingleTopic(t, &RunS3);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelOnSingleTopic, 2, 1, false) {
        TestSingleTopic(t, [](TTestWithReboots& t, const TVector<TTypedScheme>& s, const TString& r, const TTestEnvOptions& o) {
            DecorateS3(t, s, r, &Cancel, o);
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleTopic, 2, 1, false) {
        TestSingleTopic(t, [](TTestWithReboots& t, const TVector<TTypedScheme>& s, const TString& r, const TTestEnvOptions& o) {
            DecorateS3(t, s, r, &Forget, o);
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(IndexMaterialization, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            {
                EPathTypeTableIndex,
                R"(
                    TableDescription {
                      Name: "Table"
                      Columns { Name: "key" Type: "Utf8" }
                      Columns { Name: "value" Type: "Utf8" }
                      KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                      Name: "index"
                      KeyColumnNames: ["value"]
                    }
                )",
            },
        }, {{"/MyRoot/Table", ""}},
        TTestEnvOptions().EnableIndexMaterialization(true),
        "include_index_data: true");
    }

    // Async Replication
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnSingleReplication, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            TTestData::Replication()
        }, TTestData::Items(EPathTypeReplication));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelShouldSucceedOnSingleReplication, 2, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            TTestData::Replication()
        }, TTestData::Items(EPathTypeReplication));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnSingleReplication, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            TTestData::Replication()
        }, TTestData::Items(EPathTypeReplication));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnSingleTransfer, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            TTestData::Table(),
            TTestData::Transfer(),
        }, TTestData::Items(EPathTypeTransfer));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelShouldSucceedOnSingleTransfer, 2, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            TTestData::Table(),
            TTestData::Transfer(),
        }, TTestData::Items(EPathTypeTransfer));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnSingleTransfer, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            TTestData::Table(),
            TTestData::Transfer(),
        }, TTestData::Items(EPathTypeTransfer));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnSingleExternalDataSource, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            TTestData::ExternalDataSource(),
        }, TTestData::Items(EPathTypeExternalDataSource));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelShouldSucceedOnSingleExternalDataSource, 2, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            TTestData::ExternalDataSource(),
        }, TTestData::Items(EPathTypeExternalDataSource));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnSingleExternalDataSource, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            TTestData::ExternalDataSource(),
        }, TTestData::Items(EPathTypeExternalDataSource));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ShouldSucceedOnSingleExternalTable, 2, 1, false, IsFs) {
        RunExport<IsFs>(t, {
            TTestData::ExternalDataSource(),
            TTestData::ExternalTable(),
        }, TTestData::Items(EPathTypeExternalTable));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(CancelShouldSucceedOnSingleExternalTable, 2, 1, false, IsFs) {
        CancelExport<IsFs>(t, {
            TTestData::ExternalDataSource(),
            TTestData::ExternalTable(),
        }, TTestData::Items(EPathTypeExternalTable));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(ForgetShouldSucceedOnSingleExternalTable, 2, 1, false, IsFs) {
        ForgetExport<IsFs>(t, {
            TTestData::ExternalDataSource(),
            TTestData::ExternalTable(),
        }, TTestData::Items(EPathTypeExternalTable));
    }

    // Complex S3-only tests: System view with permissions
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSystemViewPermissions, 2, 1, false) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        t.GetTestEnvOptions().EnablePermissionsExport(true);
        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().FeatureFlags.SetEnableSysViewPermissionsExport(true);

                NACLib::TDiffACL diffACL;
                diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user0@builtin");
                TestModifyACL(runtime, ++t.TxId, "/MyRoot/.sys", "partition_stats", diffACL.SerializeAsString(), "user0@builtin");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestExport(runtime, ++t.TxId, "/MyRoot",
                Sprintf(R"(
                    ExportToS3Settings {
                        endpoint: "localhost:%d"
                        scheme: HTTP
                        items {
                            source_path: "/MyRoot/.sys/partition_stats"
                            destination_prefix: "/partition_stats"
                        }
                    }
                )", port)
            );

            const ui64 exportId = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, exportId);

            {
                TInactiveZone inactive(activeZone);

                auto response = TestGetExport(runtime, exportId, "/MyRoot", {
                    Ydb::StatusIds::SUCCESS,
                    Ydb::StatusIds::NOT_FOUND
                });

                if (response.GetResponse().GetEntry().GetStatus() == Ydb::StatusIds::NOT_FOUND) {
                    return;
                }

                auto* sysviewPermissions = s3Mock.GetData().FindPtr("/partition_stats/permissions.pb");
                UNIT_ASSERT(sysviewPermissions);

                TestForgetExport(runtime, ++t.TxId, "/MyRoot", exportId);
                t.TestEnv->TestWaitNotification(runtime, exportId);

                TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnSystemViewPermissions, 2, 1, false) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        t.GetTestEnvOptions().EnablePermissionsExport(true);
        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().FeatureFlags.SetEnableSysViewPermissionsExport(true);

                NACLib::TDiffACL diffACL;
                diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user0@builtin");
                TestModifyACL(runtime, ++t.TxId, "/MyRoot/.sys", "partition_stats", diffACL.SerializeAsString(), "user0@builtin");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestExport(runtime, ++t.TxId, "/MyRoot",
                Sprintf(R"(
                    ExportToS3Settings {
                        endpoint: "localhost:%d"
                        scheme: HTTP
                        items {
                            source_path: "/MyRoot/.sys/partition_stats"
                            destination_prefix: "/partition_stats"
                        }
                    }
                )", port)
            );

            const ui64 exportId = t.TxId;

            t.TestEnv->ReliablePropose(runtime, CancelExportRequest(++t.TxId, "/MyRoot", exportId), {
                Ydb::StatusIds::SUCCESS,
                Ydb::StatusIds::NOT_FOUND
            });
            t.TestEnv->TestWaitNotification(runtime, exportId);

            {
                TInactiveZone inactive(activeZone);

                auto response = TestGetExport(runtime, exportId, "/MyRoot", {
                    Ydb::StatusIds::SUCCESS,
                    Ydb::StatusIds::CANCELLED,
                    Ydb::StatusIds::NOT_FOUND
                });

                if (response.GetResponse().GetEntry().GetStatus() == Ydb::StatusIds::NOT_FOUND) {
                    return;
                }

                TestForgetExport(runtime, ++t.TxId, "/MyRoot", exportId);
                t.TestEnv->TestWaitNotification(runtime, exportId);

                TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSystemViewPermissions, 2, 1, false) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        t.GetTestEnvOptions().EnablePermissionsExport(true);
        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().FeatureFlags.SetEnableSysViewPermissionsExport(true);

                NACLib::TDiffACL diffACL;
                diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user0@builtin");
                TestModifyACL(runtime, ++t.TxId, "/MyRoot/.sys", "partition_stats", diffACL.SerializeAsString(), "user0@builtin");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestExport(runtime, ++t.TxId, "/MyRoot",
                    Sprintf(R"(
                        ExportToS3Settings {
                            endpoint: "localhost:%d"
                            scheme: HTTP
                            items {
                                source_path: "/MyRoot/.sys/partition_stats"
                                destination_prefix: "/partition_stats"
                            }
                        }
                    )", port)
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            const ui64 exportId = t.TxId;

            t.TestEnv->ReliablePropose(runtime, ForgetExportRequest(++t.TxId, "/MyRoot", exportId), {
                Ydb::StatusIds::SUCCESS,
            });
            t.TestEnv->TestWaitNotification(runtime, exportId);

            {
                TInactiveZone inactive(activeZone);
                TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
            }
        });
    }
}

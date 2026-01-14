#include <ydb/core/tx/schemeshard/ut_helpers/export_reboots_common.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/util/aws.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <library/cpp/testing/hook/hook.h>

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
    using TUnderlying = std::function<void(const TVector<TTypedScheme>&, const TString&, TTestWithReboots&)>;

    void Decorate(const TVector<TTypedScheme>& schemeObjects, const TString& request,
        TUnderlying func, const TTestEnvOptions& opts)
    {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.GetTestEnvOptions() = opts;
        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        func(schemeObjects, Sprintf(request.c_str(), port), t);
    }

    void RunS3(const TVector<TTypedScheme>& schemeObjects, const TString& request,
        const TTestEnvOptions& opts = TTestWithReboots::GetDefaultTestEnvOptions())
    {
        Decorate(schemeObjects, request, &Run, opts);
    }

    void CancelS3(const TVector<TTypedScheme>& schemeObjects, const TString& request,
        const TTestEnvOptions& opts = TTestWithReboots::GetDefaultTestEnvOptions())
    {
        Decorate(schemeObjects, request, &Cancel, opts);
    }

    void ForgetS3(const TVector<TTypedScheme>& schemeObjects, const TString& request,
        const TTestEnvOptions& opts = TTestWithReboots::GetDefaultTestEnvOptions())
    {
        Decorate(schemeObjects, request, &Forget, opts);
    }

    Y_UNIT_TEST(ShouldSucceedOnSingleShardTable) {
        RunS3({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(ShouldSucceedOnMultiShardTable) {
        RunS3({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(ShouldSucceedOnSingleTable) {
        // same as ShouldSucceedOnSingleShardTable
    }

    Y_UNIT_TEST(ShouldSucceedOnManyTables) {
        RunS3({
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
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table1"
                destination_prefix: "table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_prefix: "table2"
              }
            }
        )");
    }

    Y_UNIT_TEST(ShouldSucceedOnSingleView) {
        RunS3({
            {
                EPathTypeView,
                R"(
                    Name: "View"
                    QueryText: "some query"
                )"
            }
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/View"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(ShouldSucceedOnViewsAndTables) {
        RunS3({
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
        }, R"(
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
        )");
    }

    Y_UNIT_TEST(ShouldSucceedOnViewsAndTablesPermissions) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
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

    Y_UNIT_TEST(CancelShouldSucceedOnSingleShardTable) {
        CancelS3({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(CancelShouldSucceedOnMultiShardTable) {
        CancelS3({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(CancelShouldSucceedOnSingleTable) {
        // same as CancelShouldSucceedOnSingleShardTable
    }

    Y_UNIT_TEST(CancelShouldSucceedOnManyTables) {
        CancelS3({
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
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table1"
                destination_prefix: "table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_prefix: "table2"
              }
            }
        )");
    }

    Y_UNIT_TEST(CancelShouldSucceedOnSingleView) {
        CancelS3({
            {
                EPathTypeView,
                R"(
                    Name: "View"
                    QueryText: "some query"
                )"
            }
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/View"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(CancelShouldSucceedOnViewsAndTables) {
        CancelS3({
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
        }, R"(
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
        )");
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleShardTable) {
        ForgetS3({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnMultiShardTable) {
        ForgetS3({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleTable) {
        // same as ForgetShouldSucceedOnSingleShardTable
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnManyTables) {
        ForgetS3({
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
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table1"
                destination_prefix: "table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_prefix: "table2"
              }
            }
        )");
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleView) {
        ForgetS3({
            {
                EPathTypeView,
                R"(
                    Name: "View"
                    QueryText: "some query"
                )"
            }
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/View"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnViewsAndTables) {
        ForgetS3({
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
        }, R"(
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
        )");
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

        static TString Request(EPathType pathType = EPathType::EPathTypeTable) {
            switch (pathType) {
            case EPathType::EPathTypeTable:
                return RequestStringTable;
            case EPathType::EPathTypeReplication:
                return RequestStringReplication;
            case EPathType::EPathTypeTransfer:
                return RequestStringTransfer;
            case EPathType::EPathTypeExternalDataSource:
                return RequestStringExternalDataSource;
            case EPathType::EPathTypeExternalTable:
                return RequestStringExternalTable;
            default:
                Y_ABORT("not supported");
            }

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

        static const TString RequestStringTable;
        static const TString RequestStringReplication;
        static const TString RequestStringTransfer;
        static const TString RequestStringExternalDataSource;
        static const TString RequestStringExternalTable;

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

    const TString TTestData::RequestStringTable = R"(
        ExportToS3Settings {
            endpoint: "localhost:%d"
            scheme: HTTP
            items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
            }
        }
    )";

    const TString TTestData::RequestStringReplication = R"(
        ExportToS3Settings {
            endpoint: "localhost:%d"
            scheme: HTTP
            items {
                source_path: "/MyRoot/Replication"
                destination_prefix: ""
            }
        }
    )";

    const TString TTestData::RequestStringTransfer = R"(
        ExportToS3Settings {
            endpoint: "localhost:%d"
            scheme: HTTP
            items {
                source_path: "/MyRoot/Transfer"
                destination_prefix: ""
            }
        }
    )";

    const TString TTestData::RequestStringExternalDataSource = R"(
        ExportToS3Settings {
            endpoint: "localhost:%d"
            scheme: HTTP
            items {
                source_path: "/MyRoot/DataSource"
                destination_prefix: ""
            }
        }
    )";

    const TString TTestData::RequestStringExternalTable = R"(
        ExportToS3Settings {
            endpoint: "localhost:%d"
            scheme: HTTP
            items {
                source_path: "/MyRoot/ExternalTable"
                destination_prefix: ""
            }
        }
    )";

    Y_UNIT_TEST(ShouldSucceedOnSingleShardTableWithChangefeed) {
        RunS3({
            TTestData::Table(),
            TTestData::Changefeed()
        }, TTestData::Request());
    }

    Y_UNIT_TEST(CancelOnSingleShardTableWithChangefeed) {
        CancelS3({
            TTestData::Table(),
            TTestData::Changefeed()
        }, TTestData::Request());
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleShardTableWithChangefeed) {
        ForgetS3({
            TTestData::Table(),
            TTestData::Changefeed()
        }, TTestData::Request());
    }

    Y_UNIT_TEST(ShouldSucceedOnSingleShardTableWithUniqueIndex) {
        RunS3({
            TTestData::IndexedTable()
        }, TTestData::Request());
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleShardTableWithUniqueIndex) {
        ForgetS3({
            TTestData::IndexedTable()
        }, TTestData::Request());
    }

    Y_UNIT_TEST(CancelShouldSucceedOnSingleShardTableWithUniqueIndex) {
        CancelS3({
            TTestData::IndexedTable()
        }, TTestData::Request());
    }

    Y_UNIT_TEST(ShouldSucceedAutoDropping) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.GetTestEnvOptions().EnableRealSystemViewPaths(false);
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

                TestExport(runtime, ++t.TxId, "/MyRoot", Sprintf(TTestData::Request().data(), port));
            }

            const ui64 exportId = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, exportId);

            {
                TInactiveZone inactive(activeZone);
                TestGetExport(runtime, exportId, "/MyRoot");
                TestRmDir(runtime, ++t.TxId, "/MyRoot", "DirA");
                auto desc = DescribePath(runtime, "/MyRoot");
                UNIT_ASSERT_EQUAL(desc.GetPathDescription().ChildrenSize(), 1);
                UNIT_ASSERT_EQUAL(desc.GetPathDescription().GetChildren(0).GetName(), "Table");
            }
        });
    }

    Y_UNIT_TEST(ShouldDisableAutoDropping) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.GetTestEnvOptions().EnableRealSystemViewPaths(false);
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

                TestExport(runtime, ++t.TxId, "/MyRoot", Sprintf(TTestData::Request().data(), port));
            }

            const ui64 exportId = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, exportId);

            {
                TInactiveZone inactive(activeZone);
                TestGetExport(runtime, exportId, "/MyRoot");
                TestRmDir(runtime, ++t.TxId, "/MyRoot", "DirA");
                auto desc = DescribePath(runtime, "/MyRoot");
                UNIT_ASSERT_EQUAL(desc.GetPathDescription().ChildrenSize(), 2);
                const auto namesVector = {desc.GetPathDescription().GetChildren(0).GetName(),
                                          desc.GetPathDescription().GetChildren(1).GetName()};
                UNIT_ASSERT(IsIn(namesVector, "Table"));
                UNIT_ASSERT(IsIn(namesVector, "export-1003"));
            }
        });
    }

    using S3Func = void (*)(const TVector<TTypedScheme>&, const TString&, const TTestEnvOptions&);

    void TestSingleTopic(S3Func func) {
        auto topic = NDescUT::TSimpleTopic(0, 2);
        func(
            {
                {
                    EPathTypePersQueueGroup,
                    topic.GetPrivateProto().DebugString()
                }
            }
            , topic.GetExportRequest()
            , TTestWithReboots::GetDefaultTestEnvOptions());
    }

    Y_UNIT_TEST(ShouldSucceedOnSingleTopic) {
        TestSingleTopic(&RunS3);
    }

    Y_UNIT_TEST(CancelOnSingleTopic) {
        TestSingleTopic(&CancelS3);
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleTopic) {
        TestSingleTopic(&ForgetS3);
    }

    Y_UNIT_TEST(IndexMaterialization) {
        RunS3({
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
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              include_index_data: true
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", TTestEnvOptions().EnableIndexMaterialization(true));
    }

    // Async Replication
    Y_UNIT_TEST(ShouldSucceedOnSingleReplication) {
        RunS3({
            TTestData::Replication()
        }, TTestData::Request(EPathTypeReplication));
    }

    Y_UNIT_TEST(CancelShouldSucceedOnSingleReplication) {
        CancelS3({
            TTestData::Replication()
        }, TTestData::Request(EPathTypeReplication));
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleReplication) {
        ForgetS3({
            TTestData::Replication()
        }, TTestData::Request(EPathTypeReplication));
    }

    // Transfer
    Y_UNIT_TEST(ShouldSucceedOnSingleTransfer) {
        RunS3({
            TTestData::Table(),
            TTestData::Transfer(),
        }, TTestData::Request(EPathTypeTransfer));
    }

    Y_UNIT_TEST(CancelShouldSucceedOnSingleTransfer) {
        CancelS3({
            TTestData::Table(),
            TTestData::Transfer(),
        }, TTestData::Request(EPathTypeTransfer));
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleTransfer) {
        ForgetS3({
            TTestData::Table(),
            TTestData::Transfer(),
        }, TTestData::Request(EPathTypeTransfer));
    }

    // External Data Source
    Y_UNIT_TEST(ShouldSucceedOnSingleExternalDataSource) {
        RunS3({
            TTestData::ExternalDataSource(),
        }, TTestData::Request(EPathTypeExternalDataSource));
    }

    Y_UNIT_TEST(CancelShouldSucceedOnSingleExternalDataSource) {
        CancelS3({
            TTestData::ExternalDataSource(),
        }, TTestData::Request(EPathTypeExternalDataSource));
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleExternalDataSource) {
        ForgetS3({
            TTestData::ExternalDataSource(),
        }, TTestData::Request(EPathTypeExternalDataSource));
    }

    // External Table
    Y_UNIT_TEST(ShouldSucceedOnSingleExternalTable) {
        RunS3({
            TTestData::ExternalDataSource(),
            TTestData::ExternalTable(),
        }, TTestData::Request(EPathTypeExternalTable));
    }

    Y_UNIT_TEST(CancelShouldSucceedOnSingleExternalTable) {
        CancelS3({
            TTestData::ExternalDataSource(),
            TTestData::ExternalTable(),
        }, TTestData::Request(EPathTypeExternalTable));
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleExternalTable) {
        ForgetS3({
            TTestData::ExternalDataSource(),
            TTestData::ExternalTable(),
        }, TTestData::Request(EPathTypeExternalTable));
    }
}

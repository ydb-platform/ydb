#include <ydb/core/tx/schemeshard/ut_helpers/export_reboots_common.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
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

    void Decorate(TTestWithReboots& t, const TVector<TTypedScheme>& schemeObjects, const TString& request,
        TUnderlying func, const TTestEnvOptions& opts)
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
        Decorate(t, schemeObjects, request, &Run, opts);
    }

    void CancelS3(TTestWithReboots& t, const TVector<TTypedScheme>& schemeObjects, const TString& request,
        const TTestEnvOptions& opts = TTestWithReboots::GetDefaultTestEnvOptions())
    {
        Decorate(t, schemeObjects, request, &Cancel, opts);
    }

    void ForgetS3(TTestWithReboots& t, const TVector<TTypedScheme>& schemeObjects, const TString& request,
        const TTestEnvOptions& opts = TTestWithReboots::GetDefaultTestEnvOptions())
    {
        Decorate(t, schemeObjects, request, &Forget, opts);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleShardTable, 2, 1, false) {
        RunS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnMultiShardTable, 2, 1, false) {
        RunS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleTable, 2, 1, false) {
        // same as ShouldSucceedOnSingleShardTable
        Y_UNUSED(t);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnManyTables, 2, 1, false) {
        RunS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleView, 2, 1, false) {
        RunS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnViewsAndTables, 2, 1, false) {
        RunS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnSingleShardTable, 2, 1, false) {
        CancelS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnMultiShardTable, 2, 1, false) {
        CancelS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnSingleTable, 2, 1, false) {
        // same as CancelShouldSucceedOnSingleShardTable
        Y_UNUSED(t);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnManyTables, 2, 1, false) {
        CancelS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnSingleView, 2, 1, false) {
        CancelS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnViewsAndTables, 2, 1, false) {
        CancelS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleShardTable, 2, 1, false) {
        ForgetS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnMultiShardTable, 2, 1, false) {
        ForgetS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleTable, 2, 1, false) {
        // same as ForgetShouldSucceedOnSingleShardTable
        Y_UNUSED(t);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnManyTables, 2, 1, false) {
        ForgetS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleView, 2, 1, false) {
        ForgetS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnViewsAndTables, 2, 1, false) {
        ForgetS3(t, {
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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleShardTableWithChangefeed, 2, 1, false) {
        RunS3(t, {
            TTestData::Table(),
            TTestData::Changefeed()
        }, TTestData::Request());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelOnSingleShardTableWithChangefeed, 2, 1, false) {
        CancelS3(t, {
            TTestData::Table(),
            TTestData::Changefeed()
        }, TTestData::Request());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleShardTableWithChangefeed, 2, 1, false) {
        ForgetS3(t, {
            TTestData::Table(),
            TTestData::Changefeed()
        }, TTestData::Request());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleShardTableWithUniqueIndex, 2, 1, false) {
        RunS3(t, {
            TTestData::IndexedTable()
        }, TTestData::Request());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleShardTableWithUniqueIndex, 2, 1, false) {
        ForgetS3(t, {
            TTestData::IndexedTable()
        }, TTestData::Request());
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnSingleShardTableWithUniqueIndex, 2, 1, false) {
        CancelS3(t, {
            TTestData::IndexedTable()
        }, TTestData::Request());
    }

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

                TestExport(runtime, ++t.TxId, "/MyRoot", Sprintf(TTestData::Request().data(), port));
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
        TestSingleTopic(t, &CancelS3);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleTopic, 2, 1, false) {
        TestSingleTopic(t, &ForgetS3);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(IndexMaterialization, 2, 1, false) {
        RunS3(t, {
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
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleReplication, 2, 1, false) {
        RunS3(t, {
            TTestData::Replication()
        }, TTestData::Request(EPathTypeReplication));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnSingleReplication, 2, 1, false) {
        CancelS3(t, {
            TTestData::Replication()
        }, TTestData::Request(EPathTypeReplication));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleReplication, 2, 1, false) {
        ForgetS3(t, {
            TTestData::Replication()
        }, TTestData::Request(EPathTypeReplication));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleTransfer, 2, 1, false) {
        RunS3(t, {
            TTestData::Table(),
            TTestData::Transfer(),
        }, TTestData::Request(EPathTypeTransfer));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnSingleTransfer, 2, 1, false) {
        CancelS3(t, {
            TTestData::Table(),
            TTestData::Transfer(),
        }, TTestData::Request(EPathTypeTransfer));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleTransfer, 2, 1, false) {
        ForgetS3(t, {
            TTestData::Table(),
            TTestData::Transfer(),
        }, TTestData::Request(EPathTypeTransfer));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleExternalDataSource, 2, 1, false) {
        RunS3(t, {
            TTestData::ExternalDataSource(),
        }, TTestData::Request(EPathTypeExternalDataSource));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnSingleExternalDataSource, 2, 1, false) {
        CancelS3(t, {
            TTestData::ExternalDataSource(),
        }, TTestData::Request(EPathTypeExternalDataSource));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleExternalDataSource, 2, 1, false) {
        ForgetS3(t, {
            TTestData::ExternalDataSource(),
        }, TTestData::Request(EPathTypeExternalDataSource));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ShouldSucceedOnSingleExternalTable, 2, 1, false) {
        RunS3(t, {
            TTestData::ExternalDataSource(),
            TTestData::ExternalTable(),
        }, TTestData::Request(EPathTypeExternalTable));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(CancelShouldSucceedOnSingleExternalTable, 2, 1, false) {
        CancelS3(t, {
            TTestData::ExternalDataSource(),
            TTestData::ExternalTable(),
        }, TTestData::Request(EPathTypeExternalTable));
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(ForgetShouldSucceedOnSingleExternalTable, 2, 1, false) {
        ForgetS3(t, {
            TTestData::ExternalDataSource(),
            TTestData::ExternalTable(),
        }, TTestData::Request(EPathTypeExternalTable));
    }

    // System view
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

                // Set permissions on the system view
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

                // Set permissions on the system view
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

                // Set permissions on the system view
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

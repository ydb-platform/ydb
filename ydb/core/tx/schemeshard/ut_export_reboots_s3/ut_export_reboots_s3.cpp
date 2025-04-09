#include <ydb/core/tx/schemeshard/ut_helpers/export_reboots_common.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/util/aws.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <util/string/printf.h>

#include <library/cpp/testing/hook/hook.h>

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

    class TestData {
    public:
        static const TTypedScheme& Table() {
            return TableScheme;
        } 

        static const TTypedScheme& Changefeed() {
            return ChangefeedScheme;
        }

        static const TString& Request() {
            return RequestString;
        }

    private:
        static const char* TableName;
        static const TTypedScheme TableScheme;
        static const TTypedScheme ChangefeedScheme;
        static const TString RequestString;
    };

    const char* TestData::TableName = "Table";

    const TTypedScheme TestData::TableScheme = TTypedScheme {
        EPathTypeTable,
        Sprintf(R"(
            Name: "%s"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", TableName)
    };

    const TTypedScheme TestData::ChangefeedScheme = TTypedScheme {
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

    const TString TestData::RequestString = R"(
        ExportToS3Settings {
            endpoint: "localhost:%d"
            scheme: HTTP
            items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
            }
        }
    )";

    Y_UNIT_TEST(ShouldSucceedOnSingleShardTableWithChangefeed) {
        RunS3({
            TestData::Table(),
            TestData::Changefeed()
        }, TestData::Request());
    }

    Y_UNIT_TEST(CancelOnSingleShardTableWithChangefeed) {
        CancelS3({
            TestData::Table(),
            TestData::Changefeed()
        }, TestData::Request());
    }

    Y_UNIT_TEST(ForgetShouldSucceedOnSingleShardTableWithChangefeed) {
        ForgetS3({
            TestData::Table(),
            TestData::Changefeed()
        }, TestData::Request());
    }
}

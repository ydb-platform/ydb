#include <ydb/core/tx/schemeshard/ut_helpers/export_reboots_common.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <util/string/printf.h>

#include <library/cpp/testing/hook/hook.h>

#include <aws/core/Aws.h>

using namespace NKikimrSchemeOp;
using namespace NKikimr::NWrappers::NTestHelpers;
using namespace NSchemeShardUT_Private;
using namespace NSchemeShardUT_Private::NExportReboots;

namespace {

Aws::SDKOptions Options;

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    Aws::InitAPI(Options);
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    Aws::ShutdownAPI(Options);
}

}

Y_UNIT_TEST_SUITE(TExportToS3WithRebootsTests) {
    using TUnderlying = std::function<void(const TVector<TTypedScheme>&, const TString&, TTestWithReboots&)>;

    void Decorate(const TVector<TTypedScheme>& schemeObjects, const TString& request, TUnderlying func) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        func(schemeObjects, Sprintf(request.c_str(), port), t);
    }

    void RunS3(const TVector<TTypedScheme>& schemeObjects, const TString& request) {
        Decorate(schemeObjects, request, &Run);
    }

    void CancelS3(const TVector<TTypedScheme>& schemeObjects, const TString& request) {
        Decorate(schemeObjects, request, &Cancel);
    }

    void ForgetS3(const TVector<TTypedScheme>& schemeObjects, const TString& request) {
        Decorate(schemeObjects, request, &Forget);
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
}

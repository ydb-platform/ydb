#include <ydb/public/lib/ydb_cli/dump/util/external_table_utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NYdb::NDump {

Y_UNIT_TEST_SUITE(ExternalTablePathMapping) {
    TString Query(TStringBuf database, TStringBuf backupRoot, TStringBuf dataSource) {
        return TStringBuilder() << "-- database: \"" << database << "\"\n"
                                << "-- backup root: \"" << backupRoot << "\"\n"
                                << "CREATE EXTERNAL TABLE IF NOT EXISTS `table` (key Uint64) WITH (\n"
                                << "  DATA_SOURCE = '" << dataSource << "',\n"
                                << "  LOCATION = 'bucket'\n);";
    }

    Y_UNIT_TEST(MapSourceInsideBackupRoot) {
        auto query = Query("/Source", "/Source/a", "/Source/a/nested/eds");
        NYql::TIssues issues;
        UNIT_ASSERT_C(RewriteCreateExternalTableQuery(query, "/Destination/restored", "/Destination/restored/table", issues,
                          EExternalTableSourceMapping::BackupRoot), issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(query, "DATA_SOURCE = '/Destination/restored/nested/eds'");
    }

    Y_UNIT_TEST(OutsideSourcePreservedAtOriginalRoot) {
        auto query = Query("/Source", "/Source/a", "/Source/shared/eds");
        NYql::TIssues issues;
        UNIT_ASSERT_C(RewriteCreateExternalTableQuery(query, "/Source/a", "/Source/a/table", issues, EExternalTableSourceMapping::BackupRoot),
            issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(query, "DATA_SOURCE = '/Source/shared/eds'");
    }

    Y_UNIT_TEST(OutsideSourceFailsWhenBackupRootMoves) {
        auto query = Query("/Source", "/Source/a", "/Source/shared/eds");
        const auto original = query;
        NYql::TIssues issues;
        UNIT_ASSERT(!RewriteCreateExternalTableQuery(query, "/Source/b", "/Source/b/table", issues, EExternalTableSourceMapping::BackupRoot));
        UNIT_ASSERT(!issues.Empty());
        UNIT_ASSERT_VALUES_EQUAL(query, original);
    }

    Y_UNIT_TEST(ComponentBoundaryIsNotStringPrefix) {
        auto query = Query("/Source", "/Source/a", "/Source/ab/eds");
        NYql::TIssues issues;
        UNIT_ASSERT(
            !RewriteCreateExternalTableQuery(query, "/Destination/b", "/Destination/b/table", issues, EExternalTableSourceMapping::BackupRoot));
    }

    Y_UNIT_TEST(MissingBackupRootFailsForMovedRestore) {
        auto query = Query("/Source", "", "/Source/eds");
        NYql::TIssues issues;
        UNIT_ASSERT(
            !RewriteCreateExternalTableQuery(query, "/Destination/b", "/Destination/b/table", issues, EExternalTableSourceMapping::BackupRoot));
    }

    Y_UNIT_TEST(InvalidArchivePathsFailWithoutChangingTheQuery) {
        struct TCase {
            TStringBuf Database;
            TStringBuf BackupRoot;
            TStringBuf Source;
            TStringBuf RestoreRoot;
        };

        for (const auto& value : {
                 TCase{ "/Source", "/Source/a", "relative/source", "/Destination/b" },
                 TCase{ "relative", "/Source/a", "/Source/a/eds", "/Destination/b" },
                 TCase{ "/Source", "relative", "/Source/a/eds", "/Destination/b" },
                 TCase{ "/Source", "/Source/a", "/Source/a/eds", "relative" },
                 TCase{ "/", "/Source/a", "/Source/a/eds", "/Destination/b" },
                 TCase{ "/Source/nested", "/Source", "/Source/eds", "/Destination/b" },
                 TCase{ "/Source", "/Elsewhere/a", "/Elsewhere/a/eds", "/Destination/b" },
                 TCase{ "/Source", "/Source/a", "/Source", "/Destination/b" },
             }) {
            auto query = Query(value.Database, value.BackupRoot, value.Source);
            const auto original = query;
            NYql::TIssues issues;
            UNIT_ASSERT(!RewriteCreateExternalTableQuery(
                query, TString(value.RestoreRoot), "/Destination/b/table", issues, EExternalTableSourceMapping::BackupRoot));
            UNIT_ASSERT(!issues.Empty());
            UNIT_ASSERT_VALUES_EQUAL(query, original);
        }
    }

    Y_UNIT_TEST(UnsafeSqlPathsFailWithoutMutatingQuery) {
        for (const auto* source : { "/Source/a/with space", "/Source/a/a'b", "/Source/a/a\\b" }) {
            auto query = Query("/Source", "/Source/a", source);
            const auto original = query;
            NYql::TIssues issues;
            UNIT_ASSERT(!RewriteCreateExternalTableQuery(
                query, "/Destination/b", "/Destination/b/table", issues, EExternalTableSourceMapping::BackupRoot));
            UNIT_ASSERT_VALUES_EQUAL(query, original);
        }
        auto query = Query("/Source", "/Source/a", "/Source/a/eds");
        const auto original = query;
        NYql::TIssues issues;
        UNIT_ASSERT(
            !RewriteCreateExternalTableQuery(query, "/Destination/b", "/Destination/b/ta`ble", issues, EExternalTableSourceMapping::BackupRoot));
        UNIT_ASSERT_VALUES_EQUAL(query, original);
    }

    Y_UNIT_TEST(UnsafeDestinationPathsFailWithoutChangingTheQuery) {
        for (const auto* root : { "/Destination/with space", "/Destination/a'b", "/Destination/a\\b" }) {
            auto query = Query("/Source", "/Source/a", "/Source/a/eds");
            const auto original = query;
            NYql::TIssues issues;
            UNIT_ASSERT(!RewriteCreateExternalTableQuery(query, root, "/Destination/b/table", issues, EExternalTableSourceMapping::BackupRoot));
            UNIT_ASSERT_VALUES_EQUAL(query, original);
        }
        for (const auto* path : { "/Destination/b/with space", "/Destination/b/a\\b" }) {
            auto query = Query("/Source", "/Source/a", "/Source/a/eds");
            const auto original = query;
            NYql::TIssues issues;
            UNIT_ASSERT(!RewriteCreateExternalTableQuery(query, "/Destination/b", path, issues, EExternalTableSourceMapping::BackupRoot));
            UNIT_ASSERT_VALUES_EQUAL(query, original);
        }
    }

    Y_UNIT_TEST(EmbeddedNulInPathIsRejected) {
        TString source = "/Source/a/eds";
        source.push_back('\0');
        source += "suffix";
        auto query = Query("/Source", "/Source/a", source);
        const auto original = query;
        NYql::TIssues issues;
        UNIT_ASSERT(
            !RewriteCreateExternalTableQuery(query, "/Destination/b", "/Destination/b/table", issues, EExternalTableSourceMapping::BackupRoot));
        UNIT_ASSERT_VALUES_EQUAL(query, original);

        query = Query("/Source", "/Source/a", "/Source/a/eds");
        TString destination = "/Destination/b/table";
        destination.push_back('\0');
        destination += "suffix";
        UNIT_ASSERT(!RewriteCreateExternalTableQuery(query, "/Destination/b", destination, issues, EExternalTableSourceMapping::BackupRoot));
    }

    Y_UNIT_TEST(UnknownMappingFailsWithoutChangingTheQuery) {
        auto query = Query("/Source", "/Source/a", "/Source/a/eds");
        const auto original = query;
        NYql::TIssues issues;
        UNIT_ASSERT(!RewriteCreateExternalTableQuery(
            query, "/Destination/b", "/Destination/b/table", issues, static_cast<EExternalTableSourceMapping>(-1)));
        UNIT_ASSERT(!issues.Empty());
        UNIT_ASSERT_VALUES_EQUAL(query, original);
    }

    Y_UNIT_TEST(InvalidCreateStatementDoesNotLeavePartialRewrite) {
        auto query = Query("/Source", "/Source/a", "/Source/a/eds");
        query.SubstGlobal("CREATE EXTERNAL TABLE IF NOT EXISTS", "CREATE SOMETHING ELSE");
        const auto original = query;
        NYql::TIssues issues;
        UNIT_ASSERT(
            !RewriteCreateExternalTableQuery(query, "/Destination/b", "/Destination/b/table", issues, EExternalTableSourceMapping::BackupRoot));
        UNIT_ASSERT_VALUES_EQUAL(query, original);
    }

    Y_UNIT_TEST(LegacyMappingRemainsDefault) {
        auto query = Query("/Source", "/Source/a", "/Source/a/eds");
        NYql::TIssues issues;
        UNIT_ASSERT_C(RewriteCreateExternalTableQuery(query, "/Destination/restored", "/Destination/restored/table", issues), issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(query, "DATA_SOURCE = '/Destination/restored/a/eds'");
    }

    Y_UNIT_TEST(LegacyMappingRejectsMissingDataSource) {
        TString query = "-- database: \"/Source\"\nCREATE EXTERNAL TABLE IF NOT EXISTS `table` (key Uint64)";
        const auto original = query;
        NYql::TIssues issues;
        UNIT_ASSERT(!RewriteCreateExternalTableQuery(query, "/Destination", "/Destination/table", issues));
        UNIT_ASSERT_VALUES_EQUAL(query, original);
    }

    Y_UNIT_TEST(S3DatabaseRootMappingRemainsUnchanged) {
        auto query = Query("/Source", "/Source", "/Source/nested/eds");
        NYql::TIssues issues;
        UNIT_ASSERT_C(RewriteCreateExternalTableQuery(query, "/Destination", "/Destination/table", issues), issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(query, "DATA_SOURCE = '/Destination/nested/eds'");
    }

}   // Y_UNIT_TEST_SUITE
}   // namespace NYdb::NDump

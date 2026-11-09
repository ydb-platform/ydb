#include "run_ydb.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/folder/tempdir.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/file.h>

Y_UNIT_TEST_SUITE(ExternalTableRestore) {
    Y_UNIT_TEST(ExternalTableMappingOptionRejectsUnsafeRestore) {
        const TString database = "/" + GetYdbDatabase();
        TTempDir tempDir;
        const TFsPath archive(tempDir.Path());
        {
            TFileOutput out(archive / "create_external_table.sql");
            out << "-- database: \"/Source\"\n"
                   "-- backup root: \"/Source/selected\"\n"
                   "CREATE EXTERNAL TABLE IF NOT EXISTS `table` (key Uint64) WITH (\n"
                   "  DATA_SOURCE = '/Source/shared/eds',\n"
                   "  LOCATION = 'bucket'\n);";
        }

        auto expectFailure = [&](const TList<TString>& args, TStringBuf expected) {
            TString error;
            int exitCode = 0;
            const auto output = RunYdb(args, {}, false, true, {}, 0, &error, &exitCode);
            UNIT_ASSERT_C(exitCode != 0, output);
            UNIT_ASSERT_STRING_CONTAINS(error, expected);
        };

        const TList<TString> common = { "tools", "restore", "--path", database, "--input", archive.GetPath() };
        TList<TString> outside = common;
        outside.push_back("--external-table-source-mapping");
        outside.push_back("backup-root");
        expectFailure(outside, "Cannot map external table data source outside a moved backup root");

        TList<TString> replace = outside;
        replace.push_back("--replace");
        expectFailure(replace, "External table backup-root mapping cannot be used with replace");

        TList<TString> invalid = common;
        invalid.push_back("--external-table-source-mapping");
        invalid.push_back("invalid");
        expectFailure(invalid, "not allowed");

        // An in-root mapping proceeds past the outside-root validation.
        {
            TFileOutput out(archive / "create_external_table.sql");
            out << "-- database: \"/Source\"\n"
                   "-- backup root: \"/Source/selected\"\n"
                   "CREATE EXTERNAL TABLE IF NOT EXISTS `table` (key Uint64) WITH (\n"
                   "  DATA_SOURCE = '/Source/selected/eds',\n"
                   "  LOCATION = 'bucket'\n);";
        }
        expectFailure(outside, "Failed to create");
    }

}   // Y_UNIT_TEST_SUITE

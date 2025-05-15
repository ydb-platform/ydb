#include "s3_backup_test_base.h"

#include <fmt/format.h>

using namespace NYdb;
using namespace fmt::literals;

class TListObjectsInS3ExportTestFixture : public TS3BackupTestFixture {
    void SetUp(NUnitTest::TTestContext& /* context */) override {
        auto res = YdbQueryClient().ExecuteQuery(R"sql(
            CREATE TABLE `/Root/Table0` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            );

            CREATE TABLE `/Root/dir1/Table1` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            );

            CREATE TABLE `/Root/dir1/dir2/Table2` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            );
        )sql", NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        // Empty dir
        auto mkdir = YdbSchemeClient().MakeDirectory("/Root/dir1/dir2/dir3").GetValueSync();
        UNIT_ASSERT_C(mkdir.IsSuccess(), mkdir.GetIssues().ToString());
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }
};

Y_UNIT_TEST_SUITE_F(ListObjectsInS3Export, TListObjectsInS3ExportTestFixture) {
    Y_UNIT_TEST(ExportWithSchemaMapping) {
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "Prefix//");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);
        }

        ValidateListObjectInS3Export({
            {"Table0", "Table0"},
            {"dir1/Table1", "dir1/Table1"},
            {"dir1/dir2/Table2", "dir1/dir2/Table2"},
        }, "Prefix");
    }

    Y_UNIT_TEST(ExportWithoutSchemaMapping) {
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/Table0", .Dst = "/Prefix/t0"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/dir1/Table1", .Dst = "/Prefix/d1/t1"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/dir1/dir2/Table2", .Dst = "/Prefix/d1/d2/t2"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);
        }

        ValidateListObjectInS3Export({
            {"t0", "t0"},
            {"d1/t1", "d1/t1"},
            {"d1/d2/t2", "d1/d2/t2"},
        }, "Prefix");

        ValidateListObjectInS3Export({
            {"t1", "t1"},
            {"d2/t2", "d2/t2"},
        }, "Prefix/d1");
    }

    Y_UNIT_TEST(ExportWithEncryption) {
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "Prefix");
            exportSettings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!")
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/Table0"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/dir1/Table1"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir1/dir2//Table2"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);
        }

        NYdb::NImport::TListObjectsInS3ExportSettings listSettings = MakeListObjectsInS3ExportSettings("Prefix//");
        listSettings
            .SymmetricKey("Cool random key!");

        ValidateListObjectInS3Export({
            {"001", "Table0"},
            {"002", "dir1/Table1"},
            {"003", "dir1/dir2/Table2"},
        }, listSettings);
    }

    Y_UNIT_TEST(ExportWithEncryptionWrongKey) {
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "Prefix");
            exportSettings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!")
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/Table0"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/dir1/Table1"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir1/dir2//Table2"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);
        }

        NYdb::NImport::TListObjectsInS3ExportSettings listSettings = MakeListObjectsInS3ExportSettings("Prefix//");
        listSettings
            .SymmetricKey("Cool and random)");

        ValidateListObjectInS3Export({
            {"001", "Table0"},
            {"002", "dir1/Table1"},
            {"003", "dir1/dir2/Table2"},
        }, listSettings);
    }
}

#include "s3_backup_test_base.h"
#include <ydb/core/backup/common/metadata.h>

#include <contrib/libs/fmt/include/fmt/format.h>
#include <ydb/library/testlib/helpers.h>

using namespace NYdb;

class TListObjectsInS3ExportTestFixture : public TS3BackupTestFixture {
    void SetUp(NUnitTest::TTestContext& /* context */) override {
        using namespace fmt::literals;
        const bool isOlap = TStringBuf{Name_}.EndsWith("+IsOlap");
        auto res = YdbQueryClient().ExecuteQuery(fmt::format(R"sql(
            CREATE TABLE `/Root/Table0` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
            );

            CREATE TABLE `/Root/dir1/Table1` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
            );

            CREATE TABLE `/Root/dir1/dir2/Table2` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
            );
        )sql", "store"_a = isOlap ? "COLUMN" : "ROW"), NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        // Empty dir
        auto mkdir = YdbSchemeClient().MakeDirectory("/Root/dir1/dir2/dir3").GetValueSync();
        UNIT_ASSERT_C(mkdir.IsSuccess(), mkdir.GetIssues().ToString());
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }
};

Y_UNIT_TEST_SUITE_F(ListObjectsInS3Export, TListObjectsInS3ExportTestFixture) {
    Y_UNIT_TEST_TWIN(ExportWithSchemaMapping, IsOlap) {
        if (IsOlap) {
            return; // TODO: fix me issue@26498
        }
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

    Y_UNIT_TEST_TWIN(ExportWithoutSchemaMapping, IsOlap) {
        if (IsOlap) {
            return; // TODO: fix me issue@26498
        }
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

    Y_UNIT_TEST_TWIN(ExportWithEncryption, IsOlap) {
        if (IsOlap) {
            return; // TODO: fix me issue@26498
        }
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

        ValidateListObjectPathsInS3Export({
            "Table0",
            "dir1/Table1",
            "dir1/dir2/Table2",
        }, listSettings);
    }

    Y_UNIT_TEST_TWIN(ExportWithWrongEncryptionKey, IsOlap) {
        if (IsOlap) {
            return; // TODO: fix me issue@26498
        }
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

        auto res = YdbImportClient().ListObjectsInS3Export(listSettings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.GetStatus() << ". Issues: " << res.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(PagingParameters, IsOlap) {
        NBackup::TSchemaMapping schemaMapping;
        constexpr size_t ItemsCount = 12000;
        constexpr size_t ItemsPerFolder = ItemsCount / 5;
        constexpr size_t ItemsPerSubfolder = ItemsPerFolder / 3;
        for (size_t i = 0; i < ItemsCount; ++i) {
            TStringBuilder objectPath;
            objectPath << "Folder" << (i % 5) << "/Subfolder" << (i % 3) << "/Object" << i;
            schemaMapping.Items.emplace_back(NBackup::TSchemaMapping::TItem{
                .ExportPrefix = TStringBuilder() << "Prefix" << i,
                .ObjectPath = objectPath,
            });
        }
        S3Mock().GetData()["/test_bucket/Prefix/SchemaMapping/mapping.json"] = schemaMapping.Serialize();

        // Without paging
        {
            NYdb::NImport::TListObjectsInS3ExportSettings listSettings = MakeListObjectsInS3ExportSettings("Prefix//");
            auto res = YdbImportClient().ListObjectsInS3Export(listSettings).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), "Status: " << res.GetStatus() << ". Issues: " << res.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(res.GetItems().size(), ItemsCount);
        }

        // From beginning to the end
        {
            auto addItemsByFilter = [&](size_t expectedItemsCount, const TString& prefix1 = {}, const TString& prefix2 = {}) {
                THashSet<TString> items;
                i64 pageSize = 42;
                TString nextPageToken;
                do {
                    NYdb::NImport::TListObjectsInS3ExportSettings listSettings = MakeListObjectsInS3ExportSettings("Prefix//");
                    if (prefix1) {
                        listSettings.AppendItem(NYdb::NImport::TListObjectsInS3ExportSettings::TItem{.Path = prefix1});
                    }
                    if (prefix2) {
                        listSettings.AppendItem(NYdb::NImport::TListObjectsInS3ExportSettings::TItem{.Path = prefix2});
                    }
                    auto res = YdbImportClient().ListObjectsInS3Export(listSettings, pageSize, nextPageToken).GetValueSync();
                    UNIT_ASSERT_C(res.IsSuccess(), "Status: " << res.GetStatus() << ". Issues: " << res.GetIssues().ToString());
                    UNIT_ASSERT_GT(res.GetItems().size(), 0);
                    UNIT_ASSERT_LE(res.GetItems().size(), static_cast<size_t>(pageSize));
                    for (const auto& item : res.GetItems()) {
                        UNIT_ASSERT_C(items.emplace(item.Path).second, "Duplicate item: {" << item.Prefix << ", " << item.Path << "}. Listing result: " << res);
                    }

                    ++pageSize; // just for fun
                    nextPageToken = res.NextPageToken();
                } while (!nextPageToken.empty());
                UNIT_ASSERT_VALUES_EQUAL(items.size(), expectedItemsCount);
            };

            addItemsByFilter(ItemsCount);
            addItemsByFilter(ItemsPerFolder, "/Folder2");
            addItemsByFilter(ItemsPerSubfolder, "Folder1/Subfolder1//");
            addItemsByFilter(ItemsPerSubfolder, "Folder1/Subfolder1//");
            addItemsByFilter(1, "Folder0/Subfolder2//Object620");
            addItemsByFilter(ItemsPerSubfolder + 1, "Folder0/Subfolder2//Object620", "Folder1/Subfolder1");
            addItemsByFilter(1, "Folder0/Subfolder2//Object620", "Folder1/Subfolder"); // treat not as a prefix, but as a directory, so Subfolder don't match anything
            addItemsByFilter(2, "Folder0/Subfolder2//Object620", "Folder1/Subfolder1/Object931");
        }
    }

    Y_UNIT_TEST_TWIN(ParametersValidation, IsOlap) {
        if (IsOlap) {
            return; // TODO: fix me issue@26498
        }
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "Prefix//");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);
        }

        {
            // No prefix
            NYdb::NImport::TListObjectsInS3ExportSettings listSettings = MakeListObjectsInS3ExportSettings("");
            auto res = YdbImportClient().ListObjectsInS3Export(listSettings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.GetStatus() << ". Issues: " << res.GetIssues().ToString());
        }

        {
            // Negative page size
            NYdb::NImport::TListObjectsInS3ExportSettings listSettings = MakeListObjectsInS3ExportSettings("Prefix");
            auto res = YdbImportClient().ListObjectsInS3Export(listSettings, -42).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.GetStatus() << ". Issues: " << res.GetIssues().ToString());
        }

        {
            // Big page size
            NYdb::NImport::TListObjectsInS3ExportSettings listSettings = MakeListObjectsInS3ExportSettings("Prefix");
            auto res = YdbImportClient().ListObjectsInS3Export(listSettings, 100500).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.GetStatus() << ". Issues: " << res.GetIssues().ToString());
        }

        {
            // Wrong page token
            NYdb::NImport::TListObjectsInS3ExportSettings listSettings = MakeListObjectsInS3ExportSettings("Prefix");
            auto res = YdbImportClient().ListObjectsInS3Export(listSettings, 42, "incorrect page token").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.GetStatus() << ". Issues: " << res.GetIssues().ToString());
        }
    }
}

#include "s3_backup_test_base.h"

using namespace NYdb;

template <bool encryptionEnabled>
class TBackupEncryptionParamsValidationTestFixture : public TS3BackupTestFixture {
public:
    TBackupEncryptionParamsValidationTestFixture() = default;

    void SetUp(NUnitTest::TTestContext& /* context */) override {
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableEncryptedExport(encryptionEnabled);

        auto res = YdbQueryClient().ExecuteQuery(R"sql(
            CREATE TABLE `/Root/ExportParamsValidation/dir1/Table1` (
                Key Uint32 NOT NULL,
                PRIMARY KEY (Key)
            );
        )sql", NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    }

    void MakeFullExport(bool encrypted = false) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "Prefix");
        if (encrypted) {
            settings.SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
        }
        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        WaitOpSuccess(res);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }
};

Y_UNIT_TEST_SUITE_F(EncryptedBackupParamsValidationTest, TBackupEncryptionParamsValidationTestFixture<true>)
{
    Y_UNIT_TEST(BadSourcePath) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "");

        settings.SourcePath("unknown").DestinationPrefix("dest");
        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::SCHEME_ERROR, res.Status().GetIssues().ToString());

        // Fix
        {
            NExport::TExportToS3Settings fixSettings = settings;
            fixSettings.SourcePath("/Root/");
            auto res = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }
    }

    Y_UNIT_TEST(NoDestination) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "");
        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());

        // Fix
        {
            NExport::TExportToS3Settings fixSettings = settings;
            fixSettings.DestinationPrefix("dest");
            auto res = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }
    }

    Y_UNIT_TEST(NoItemDestination) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "");
        settings.AppendItem({"/Root/ExportParamsValidation/dir1/Table1", ""});
        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());

        // Fix
        {
            NExport::TExportToS3Settings fixSettings = settings;
            fixSettings.Item_[0].Dst = "aaa";
            auto res = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }

        // Fix 2
        {
            NExport::TExportToS3Settings fixSettings = settings;
            fixSettings.DestinationPrefix("aaa");
            auto res = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }
    }

    Y_UNIT_TEST(NoCommonDestination) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "");
        settings.AppendItem({"/Root/ExportParamsValidation/dir1/Table1", "dest"});
        settings.SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());

        // Fix
        {
            NExport::TExportToS3Settings fixSettings = settings;
            fixSettings.DestinationPrefix("aaa");
            auto res = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }
    }

    Y_UNIT_TEST(IncorrectKeyLengthExport) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "");
        settings.DestinationPrefix("encrypted_export");
        settings.SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::CHACHA_20_POLY_1305, "123");
        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());

        // Fix
        {
            NExport::TExportToS3Settings fixSettings = settings;
            fixSettings.SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::CHACHA_20_POLY_1305, "Key is big enough to be 32 bytes");
            auto res = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }
    }

    Y_UNIT_TEST(NoSourcePrefix) {
        MakeFullExport();

        NImport::TImportFromS3Settings settings = MakeImportSettings("", "");
        // No items and no prefix

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "No source prefix specified. Don't know where to import from", res.Status().GetIssues().ToString());

        // Fix
        {
            NImport::TImportFromS3Settings fixSettings = settings;
            fixSettings.AppendItem({.Src = "Prefix/ExportParamsValidation/dir1/Table1", .Dst = "/Root/RestorePrefix/Table1"});
            auto res = YdbImportClient().ImportFromS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }

        // Fix 2
        {
            NImport::TImportFromS3Settings fixSettings = settings;
            fixSettings.SourcePrefix("Prefix").DestinationPath("/Root/RestorePrefix2");
            auto res = YdbImportClient().ImportFromS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }
    }

    Y_UNIT_TEST(EmptyImportItem) {
        MakeFullExport();

        NImport::TImportFromS3Settings settings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
        settings
            .AppendItem({.Src = "", .Dst = ""});

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Empty import item was specified", res.Status().GetIssues().ToString());

        // Fix
        {
            NImport::TImportFromS3Settings fixSettings = settings;
            fixSettings.Item_[0].SrcPath = "ExportParamsValidation/dir1/Table1";
            fixSettings.Item_[0].Dst = "/Root/RestorePrefix/RestoredTable";
            auto res = YdbImportClient().ImportFromS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }
    }

    Y_UNIT_TEST(IncorrectKeyImport) {
        MakeFullExport(true);

        NImport::TImportFromS3Settings settings = MakeImportSettings("Prefix", "Root//RestorePrefix/");
        settings
            .SymmetricKey("123");

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        WaitOpStatus(res, NYdb::EStatus::CANCELLED);

        // Fix
        {
            NImport::TImportFromS3Settings fixSettings = settings;
            fixSettings
                .SymmetricKey("Cool random key!");
            auto res = YdbImportClient().ImportFromS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }
    }

    Y_UNIT_TEST(EncryptionSettingsWithoutKeyImport) {
        MakeFullExport(true);

        NImport::TImportFromS3Settings settings = MakeImportSettings("Prefix", "Root//RestorePrefix/");
        settings
            .SymmetricKey("");

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "No encryption key specified", res.Status().GetIssues().ToString());

        // Fix
        {
            NImport::TImportFromS3Settings fixSettings = settings;
            fixSettings
                .SymmetricKey("Cool random key!");
            auto res = YdbImportClient().ImportFromS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }
    }

    Y_UNIT_TEST(NoSourcePrefixEncrypted) {
        MakeFullExport(true);

        NImport::TImportFromS3Settings settings = MakeImportSettings("", "/Root/RestorePath");
        settings
            .SymmetricKey("Cool random key!");

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "No source prefix specified", res.Status().GetIssues().ToString());

        // Fix
        {
            NImport::TImportFromS3Settings fixSettings = settings;
            fixSettings.SourcePrefix("Prefix");
            auto res = YdbImportClient().ImportFromS3(fixSettings).GetValueSync();
            WaitOpSuccess(res);
        }
    }
}

Y_UNIT_TEST_SUITE_F(EncryptedBackupParamsValidationTestFeatureDisabled, TBackupEncryptionParamsValidationTestFixture<false>) {
    Y_UNIT_TEST(EncryptionParamsSpecifiedExport) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "");
        settings
            .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");

        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Export encryption is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(CommonSourcePathSpecified) {
        NExport::TExportToS3Settings settings = MakeExportSettings("/Root/ExportParamsValidation", "");

        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Source path is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(CommonDestPrefixSpecified) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "Prefix");

        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Destination prefix is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(EncryptionParamsSpecifiedImport) {
        NImport::TImportFromS3Settings settings = MakeImportSettings("", "");
        settings
            .SymmetricKey("Cool random key!");

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Export encryption is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(CommonSourcePrefixSpecified) {
        NImport::TImportFromS3Settings settings = MakeImportSettings("Prefix", "");

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Source prefix is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(CommonDestPathSpecified) {
        NImport::TImportFromS3Settings settings = MakeImportSettings("", "/Root/DestPath");

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Destination path is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(SrcPrefixAndSrcPathSpecified) {
        NImport::TImportFromS3Settings settings = MakeImportSettings("", "");
        settings.AppendItem({.Src = "Prefix/Table1", .Dst = "/Root/Table", .SrcPath = "/src/path"});

        UNIT_ASSERT_EXCEPTION_CONTAINS(YdbImportClient().ImportFromS3(settings), NYdb::TContractViolation, "Invalid item: both source prefix and source path are set: \"Prefix/Table1\" and \"/src/path\"");
    }
}

class TBackupEncryptionTestFixture : public TS3BackupTestFixture {
    void SetUp(NUnitTest::TTestContext& /* context */) override {
        auto res = YdbQueryClient().ExecuteQuery(R"sql(
            CREATE TABLE `/Root/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable` (
                Key Uint32 NOT NULL,
                Value Text,
                PRIMARY KEY (Key)
            );
        )sql", NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        InsertData();
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

protected:
    void InsertData(const TString& tableName = "/Root/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable") {
        TStringBuilder sql;
        sql << "INSERT INTO `" << tableName << "` (Key, Value) VALUES (1, \"Encrypted hello world\");";
        auto res = YdbQueryClient().ExecuteQuery(sql, NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    }

    void CheckRestoredData(const TString& tableName = "/Root/EncryptedExportAndImport/RestoredExport/dir2/EncryptedExportAndImportTable") {
        TStringBuilder sql;
        sql << "SELECT * FROM `" << tableName << "`";
        auto result = YdbQueryClient().ExecuteQuery(sql, NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetStatus() << ": " << result.GetIssues().ToString());

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto resultSet = result.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Key").GetUint32(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Value").GetOptionalUtf8(), "Encrypted hello world");
    }
};

Y_UNIT_TEST_SUITE_F(EncryptedExportTest, TBackupEncryptionTestFixture) {
    Y_UNIT_TEST(EncryptedExportAndImport)
    {
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/EncryptedExportAndImport/dir1", "EncryptedExport");
            exportSettings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");

            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/EncryptedExport/001/data_00.csv.enc",
                "/test_bucket/EncryptedExport/001/metadata.json.enc",
                "/test_bucket/EncryptedExport/001/scheme.pb.enc",
                "/test_bucket/EncryptedExport/SchemaMapping/mapping.json.enc",
                "/test_bucket/EncryptedExport/SchemaMapping/metadata.json.enc",
                "/test_bucket/EncryptedExport/metadata.json",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("EncryptedExport", "/Root/EncryptedExportAndImport/RestoredExport");
            importSettings
                .SymmetricKey("Cool random key!");

            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            CheckRestoredData();
        }
    }

    Y_UNIT_TEST(EncryptionAndCompression) {
        {
            NExport::TExportToS3Settings settings = MakeExportSettings("/Root/EncryptedExportAndImport/dir1/dir2", "Prefix");
            settings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!")
                .Compression("zstd");

            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
                "/test_bucket/Prefix/001/metadata.json.enc",
                "/test_bucket/Prefix/001/scheme.pb.enc",
                "/test_bucket/Prefix/001/data_00.csv.zst.enc",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/Restored");
            importSettings
                .SymmetricKey("Cool random key!");

            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            CheckRestoredData("/Root/Restored/EncryptedExportAndImportTable");
        }
    }

    Y_UNIT_TEST(EncryptionAndChecksum) {
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableChecksumsExport(true);

        {
            NExport::TExportToS3Settings settings = MakeExportSettings("/Root/EncryptedExportAndImport/dir1/dir2", "Prefix");
            settings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");

            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/001/metadata.json.enc",
                "/test_bucket/Prefix/001/metadata.json.sha256",
                "/test_bucket/Prefix/001/scheme.pb.enc",
                "/test_bucket/Prefix/001/scheme.pb.sha256",
                "/test_bucket/Prefix/001/data_00.csv.enc",
                "/test_bucket/Prefix/001/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/Restored");
            importSettings
                .SymmetricKey("Cool random key!");

            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            CheckRestoredData("/Root/Restored/EncryptedExportAndImportTable");

            ModifyChecksumAndCheckThatImportFails({
                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/001/metadata.json.sha256",
                "/test_bucket/Prefix/001/scheme.pb.sha256",
                "/test_bucket/Prefix/001/data_00.csv.sha256",
            }, importSettings);
        }
    }

    Y_UNIT_TEST(EncryptionChecksumAndCompression) {
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableChecksumsExport(true);

        {
            NExport::TExportToS3Settings settings = MakeExportSettings("/Root/EncryptedExportAndImport/dir1/dir2", "Prefix");
            settings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!")
                .Compression("zstd");

            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/001/metadata.json.enc",
                "/test_bucket/Prefix/001/metadata.json.sha256",
                "/test_bucket/Prefix/001/scheme.pb.enc",
                "/test_bucket/Prefix/001/scheme.pb.sha256",
                "/test_bucket/Prefix/001/data_00.csv.zst.enc",
                "/test_bucket/Prefix/001/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/Restored");
            importSettings
                .SymmetricKey("Cool random key!");

            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            CheckRestoredData("/Root/Restored/EncryptedExportAndImportTable");

            ModifyChecksumAndCheckThatImportFails({
                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/001/metadata.json.sha256",
                "/test_bucket/Prefix/001/scheme.pb.sha256",
                "/test_bucket/Prefix/001/data_00.csv.sha256",
            }, importSettings);
        }
    }

    Y_UNIT_TEST(ChangefeedEncryption) {
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableChecksumsExport(true);
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableChangefeedsExport(true);
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableChangefeedsImport(true);

        auto res = YdbQueryClient().ExecuteQuery(R"sql(
            ALTER TABLE `/Root/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable` ADD CHANGEFEED `TestChangeFeed1` WITH (
                FORMAT = 'JSON',
                MODE = 'UPDATES'
            );

            ALTER TABLE `/Root/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable` ADD CHANGEFEED `TestChangeFeed2` WITH (
                FORMAT = 'JSON',
                MODE = 'UPDATES'
            );
        )sql", NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        {
            NExport::TExportToS3Settings settings = MakeExportSettings("", "Prefix");
            settings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");

            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/001/metadata.json.enc",
                "/test_bucket/Prefix/001/metadata.json.sha256",
                "/test_bucket/Prefix/001/scheme.pb.enc",
                "/test_bucket/Prefix/001/scheme.pb.sha256",
                "/test_bucket/Prefix/001/data_00.csv.enc",
                "/test_bucket/Prefix/001/data_00.csv.sha256",
                "/test_bucket/Prefix/001/001/changefeed_description.pb.enc",
                "/test_bucket/Prefix/001/001/changefeed_description.pb.sha256",
                "/test_bucket/Prefix/001/001/topic_description.pb.enc",
                "/test_bucket/Prefix/001/001/topic_description.pb.sha256",
                "/test_bucket/Prefix/001/002/changefeed_description.pb.enc",
                "/test_bucket/Prefix/001/002/changefeed_description.pb.sha256",
                "/test_bucket/Prefix/001/002/topic_description.pb.enc",
                "/test_bucket/Prefix/001/002/topic_description.pb.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/Restored");
            importSettings
                .SymmetricKey("Cool random key!");

            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);
        }

        auto changeFeed1Describe = YdbSchemeClient().DescribePath("/Root/Restored/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable/TestChangeFeed1").GetValueSync();
        UNIT_ASSERT_C(changeFeed1Describe.IsSuccess(), changeFeed1Describe.GetIssues().ToString());

        auto changeFeed2Describe = YdbSchemeClient().DescribePath("/Root/Restored/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable/TestChangeFeed2").GetValueSync();
        UNIT_ASSERT_C(changeFeed2Describe.IsSuccess(), changeFeed2Describe.GetIssues().ToString());
    }
}

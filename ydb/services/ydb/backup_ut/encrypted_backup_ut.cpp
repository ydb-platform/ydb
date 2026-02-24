#include "s3_backup_test_base.h"
#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/wrappers/ut_helpers/fs_mock.h>

#include <library/cpp/streams/zstd/zstd.h>

#include <util/folder/tempdir.h>
#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/stream/buffer.h>
#include <util/stream/str.h>

#include <contrib/libs/fmt/include/fmt/format.h>
#include <ydb/library/testlib/helpers.h>

using namespace NYdb;

template <bool encryptionEnabled>
class TBackupEncryptionParamsValidationTestFixture : public TS3BackupTestFixture {
public:
    TBackupEncryptionParamsValidationTestFixture() = default;

    void SetUp(NUnitTest::TTestContext& /* context */) override {
        using namespace fmt::literals;
        const bool isOlap = TStringBuf{Name_}.EndsWith("+IsOlap");
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableEncryptedExport(encryptionEnabled);

        auto res = YdbQueryClient().ExecuteQuery(fmt::format(R"sql(
            CREATE TABLE `/Root/ExportParamsValidation/dir1/Table1` (
                Key Uint32 NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );
        )sql", "store"_a = isOlap ? "COLUMN" : "ROW"
        , "partition_count"_a = isOlap ? ", PARTITION_COUNT = 1" : ""), NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    }

    void MakeFullExport(bool encrypted = false) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "Prefix");
        if (encrypted) {
            settings.SymmetricEncryption(NExport::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
        }
        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        WaitOpSuccess(res);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }
};

Y_UNIT_TEST_SUITE_F(EncryptedBackupParamsValidationTest, TBackupEncryptionParamsValidationTestFixture<true>)
{
    Y_UNIT_TEST_TWIN(BadSourcePath, IsOlap) {
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

    Y_UNIT_TEST_TWIN(NoDestination, IsOlap) {
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

    Y_UNIT_TEST_TWIN(NoItemDestination, IsOlap) {
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

    Y_UNIT_TEST_TWIN(NoCommonDestination, IsOlap) {
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

    Y_UNIT_TEST_TWIN(IncorrectKeyLengthExport, IsOlap) {
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

    Y_UNIT_TEST_TWIN(NoSourcePrefix, IsOlap) {
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

    Y_UNIT_TEST_TWIN(EmptyImportItem, IsOlap) {
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

    Y_UNIT_TEST_TWIN(IncorrectKeyImport, IsOlap) {
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

    Y_UNIT_TEST_TWIN(EncryptionSettingsWithoutKeyImport, IsOlap) {
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

    Y_UNIT_TEST_TWIN(NoSourcePrefixEncrypted, IsOlap) {
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
    Y_UNIT_TEST_TWIN(EncryptionParamsSpecifiedExport, IsOlap) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "");
        settings
            .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");

        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Export encryption is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(CommonSourcePathSpecified, IsOlap) {
        NExport::TExportToS3Settings settings = MakeExportSettings("/Root/ExportParamsValidation", "");

        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Source path is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(CommonDestPrefixSpecified, IsOlap) {
        NExport::TExportToS3Settings settings = MakeExportSettings("", "Prefix");

        auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Destination prefix is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(EncryptionParamsSpecifiedImport, IsOlap) {
        NImport::TImportFromS3Settings settings = MakeImportSettings("", "");
        settings
            .SymmetricKey("Cool random key!");

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Export encryption is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(CommonSourcePrefixSpecified, IsOlap) {
        NImport::TImportFromS3Settings settings = MakeImportSettings("Prefix", "");

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Source prefix is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(CommonDestPathSpecified, IsOlap) {
        NImport::TImportFromS3Settings settings = MakeImportSettings("", "/Root/DestPath");

        auto res = YdbImportClient().ImportFromS3(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Destination path is not supported in current configuration", res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(SrcPrefixAndSrcPathSpecified, IsOlap) {
        NImport::TImportFromS3Settings settings = MakeImportSettings("", "");
        settings.AppendItem({.Src = "Prefix/Table1", .Dst = "/Root/Table", .SrcPath = "/src/path"});

        UNIT_ASSERT_EXCEPTION_CONTAINS(YdbImportClient().ImportFromS3(settings), NYdb::TContractViolation, "Invalid item: both source prefix and source path are set: \"Prefix/Table1\" and \"/src/path\"");
    }
}

class TBackupEncryptionTestFixture : public TS3BackupTestFixture {
    void SetUp(NUnitTest::TTestContext& /* context */) override {
        using namespace fmt::literals;
        const bool isOlap = TStringBuf{Name_}.EndsWith("+IsOlap");
        auto res = YdbQueryClient().ExecuteQuery(fmt::format(R"sql(
            CREATE TABLE `/Root/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable` (
                Key Uint32 NOT NULL,
                Value Text,
                PRIMARY KEY (Key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );
        )sql", "store"_a = isOlap ? "COLUMN" : "ROW"
        , "partition_count"_a = isOlap ? ", PARTITION_COUNT = 1" : ""), NQuery::TTxControl::NoTx()).GetValueSync();
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
    Y_UNIT_TEST_TWIN(EncryptedExportAndImport, IsOlap)
    {
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/EncryptedExportAndImport/dir1", "EncryptedExport");
            exportSettings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");

            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/EncryptedExport/001/data_00.csv.enc",
                "/test_bucket/EncryptedExport/001/data_00.csv.sha256",
                "/test_bucket/EncryptedExport/001/metadata.json.enc",
                "/test_bucket/EncryptedExport/001/metadata.json.sha256",
                "/test_bucket/EncryptedExport/001/scheme.pb.enc",
                "/test_bucket/EncryptedExport/001/scheme.pb.sha256",
                "/test_bucket/EncryptedExport/001/permissions.pb.enc",
                "/test_bucket/EncryptedExport/001/permissions.pb.sha256",
                "/test_bucket/EncryptedExport/SchemaMapping/mapping.json.enc",
                "/test_bucket/EncryptedExport/SchemaMapping/mapping.json.sha256",
                "/test_bucket/EncryptedExport/SchemaMapping/metadata.json.enc",
                "/test_bucket/EncryptedExport/SchemaMapping/metadata.json.sha256",
                "/test_bucket/EncryptedExport/metadata.json",
                "/test_bucket/EncryptedExport/metadata.json.sha256",
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

    Y_UNIT_TEST_TWIN(EncryptionAndCompression, IsOlap) {
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
                "/test_bucket/Prefix/001/permissions.pb.enc",
                "/test_bucket/Prefix/001/permissions.pb.sha256",
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
        }
    }

    Y_UNIT_TEST_TWIN(EncryptionAndChecksum, IsOlap) {
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
                "/test_bucket/Prefix/001/permissions.pb.enc",
                "/test_bucket/Prefix/001/permissions.pb.sha256",
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

    Y_UNIT_TEST_TWIN(EncryptionChecksumAndCompression, IsOlap) {
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
                "/test_bucket/Prefix/001/permissions.pb.enc",
                "/test_bucket/Prefix/001/permissions.pb.sha256",
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
                "/test_bucket/Prefix/001/permissions.pb.enc",
                "/test_bucket/Prefix/001/permissions.pb.sha256",
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

    Y_UNIT_TEST(TopicEncryption) {
        TString query = R"sql(
            CREATE TOPIC `/Root/EncryptedExportAndImport/dir1/dir2/dir3/Topic` (
                CONSUMER Consumer
            );
        )sql";

        TSet<TString> s3FileList = {
            "/test_bucket/Prefix/metadata.json",
            "/test_bucket/Prefix/metadata.json.sha256",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
            "/test_bucket/Prefix/001/create_topic.pb.enc",
            "/test_bucket/Prefix/001/create_topic.pb.sha256",
            "/test_bucket/Prefix/001/metadata.json.enc",
            "/test_bucket/Prefix/001/metadata.json.sha256",
            "/test_bucket/Prefix/001/permissions.pb.enc",
            "/test_bucket/Prefix/001/permissions.pb.sha256",
        };

        TestSchemeObjectEncryptedExportImport(query, "Topic", s3FileList);
    }

    Y_UNIT_TEST_TWIN(ViewEncryption, IsOlap) {
        TString query = R"sql(
            CREATE VIEW `/Root/EncryptedExportAndImport/dir1/dir2/dir3/View`
                WITH (security_invoker = TRUE) AS
                    SELECT Value FROM `/Root/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable`
                        WHERE Key = 42;
        )sql";

        TSet<TString> s3FileList = {
            "/test_bucket/Prefix/metadata.json",
            "/test_bucket/Prefix/metadata.json.sha256",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
            "/test_bucket/Prefix/001/create_view.sql.enc",
            "/test_bucket/Prefix/001/create_view.sql.sha256",
            "/test_bucket/Prefix/001/metadata.json.enc",
            "/test_bucket/Prefix/001/metadata.json.sha256",
            "/test_bucket/Prefix/001/permissions.pb.enc",
            "/test_bucket/Prefix/001/permissions.pb.sha256",
        };

        TestSchemeObjectEncryptedExportImport(query, "View", s3FileList);
    }

    Y_UNIT_TEST_TWIN(ReplicationEncryption, IsOlap) {
        TString query = Sprintf(R"sql(
            CREATE ASYNC REPLICATION `/Root/EncryptedExportAndImport/dir1/dir2/dir3/Replication`
            FOR `/Root/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable` AS `/Root/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTableReplica`
            WITH (
                CONNECTION_STRING = 'grpc://localhost:%u/?database=/Root/EncryptedExportAndImport'
            );
        )sql", Server().GetPort());

        TSet<TString> s3FileList = {
            "/test_bucket/Prefix/001/create_async_replication.sql.enc",
            "/test_bucket/Prefix/001/create_async_replication.sql.sha256",
            "/test_bucket/Prefix/001/metadata.json.enc",
            "/test_bucket/Prefix/001/metadata.json.sha256",
            "/test_bucket/Prefix/001/permissions.pb.enc",
            "/test_bucket/Prefix/001/permissions.pb.sha256",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
            "/test_bucket/Prefix/metadata.json",
            "/test_bucket/Prefix/metadata.json.sha256"
        };

        TestSchemeObjectEncryptedExportImport(query, "Replication", s3FileList);
    }

    Y_UNIT_TEST_TWIN(TransferEncryption, IsOlap) {
        auto query = Sprintf(R"sql(
            CREATE TOPIC `/Root/EncryptedExportAndImport/dir1/dir2/dir3/Topic`;

            $transformation_lambda = ($msg) -> {
                return [
                    <|
                        partition: $msg._partition,
                        offset: $msg._offset,
                        message: CAST($msg._data AS Utf8)
                    |>
                ];
            };

            CREATE TRANSFER `/Root/EncryptedExportAndImport/dir1/dir2/dir3/Transfer`
                FROM `/Root/EncryptedExportAndImport/dir1/dir2/dir3/Topic` TO `/Root/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable` USING $transformation_lambda
            WITH (
                CONNECTION_STRING = 'grpc://localhost:%u/?database=/Root/EncryptedExportAndImport'
            );
        )sql", Server().GetPort());

        TSet<TString> s3FileList = {
            "/test_bucket/Prefix/001/create_transfer.sql.enc",
            "/test_bucket/Prefix/001/create_transfer.sql.sha256",
            "/test_bucket/Prefix/001/metadata.json.enc",
            "/test_bucket/Prefix/001/metadata.json.sha256",
            "/test_bucket/Prefix/001/permissions.pb.enc",
            "/test_bucket/Prefix/001/permissions.pb.sha256",
            "/test_bucket/Prefix/002/create_topic.pb.enc",
            "/test_bucket/Prefix/002/create_topic.pb.sha256",
            "/test_bucket/Prefix/002/metadata.json.enc",
            "/test_bucket/Prefix/002/metadata.json.sha256",
            "/test_bucket/Prefix/002/permissions.pb.enc",
            "/test_bucket/Prefix/002/permissions.pb.sha256",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
            "/test_bucket/Prefix/metadata.json",
            "/test_bucket/Prefix/metadata.json.sha256"
        };

        TestSchemeObjectEncryptedExportImport(query, "Transfer", s3FileList);
    }

    Y_UNIT_TEST_TWIN(ExternalDataSourceEncryption, IsOlap) {
        auto query = R"sql(
            CREATE EXTERNAL DATA SOURCE `/Root/EncryptedExportAndImport/dir1/dir2/dir3/ExternalDataSource` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="https://object_storage_domain/bucket/",
                AUTH_METHOD="NONE"
            );
        )sql";

        TSet<TString> s3FileList = {
            "/test_bucket/Prefix/001/create_external_data_source.sql.enc",
            "/test_bucket/Prefix/001/create_external_data_source.sql.sha256",
            "/test_bucket/Prefix/001/metadata.json.enc",
            "/test_bucket/Prefix/001/metadata.json.sha256",
            "/test_bucket/Prefix/001/permissions.pb.enc",
            "/test_bucket/Prefix/001/permissions.pb.sha256",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
            "/test_bucket/Prefix/metadata.json",
            "/test_bucket/Prefix/metadata.json.sha256"
        };

        TestSchemeObjectEncryptedExportImport(query, "ExternalDataSource", s3FileList);
    }

    Y_UNIT_TEST_TWIN(ExternalTableEncryption, IsOlap) {
        auto query = R"sql(
            CREATE EXTERNAL DATA SOURCE `/Root/EncryptedExportAndImport/dir1/dir2/dir3/ExternalDataSource` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="https://object_storage_domain/bucket/",
                AUTH_METHOD="NONE"
            );

            CREATE EXTERNAL TABLE `/Root/EncryptedExportAndImport/dir1/dir2/dir3/ExternalTable` (
                key Utf8 NOT NULL,
                value Utf8 NOT NULL
            ) WITH (
                DATA_SOURCE="/Root/EncryptedExportAndImport/dir1/dir2/dir3/ExternalDataSource",
                LOCATION="folder",
                FORMAT="csv_with_names",
                COMPRESSION="gzip"
            );
        )sql";

        TSet<TString> s3FileList = {
            "/test_bucket/Prefix/001/create_external_data_source.sql.enc",
            "/test_bucket/Prefix/001/create_external_data_source.sql.sha256",
            "/test_bucket/Prefix/001/metadata.json.enc",
            "/test_bucket/Prefix/001/metadata.json.sha256",
            "/test_bucket/Prefix/001/permissions.pb.enc",
            "/test_bucket/Prefix/001/permissions.pb.sha256",
            "/test_bucket/Prefix/002/create_external_table.sql.enc",
            "/test_bucket/Prefix/002/create_external_table.sql.sha256",
            "/test_bucket/Prefix/002/metadata.json.enc",
            "/test_bucket/Prefix/002/metadata.json.sha256",
            "/test_bucket/Prefix/002/permissions.pb.enc",
            "/test_bucket/Prefix/002/permissions.pb.sha256",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
            "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
            "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
            "/test_bucket/Prefix/metadata.json",
            "/test_bucket/Prefix/metadata.json.sha256"
        };

        TestSchemeObjectEncryptedExportImport(query, "ExternalTable", s3FileList);
    }
}

class TBackupEncryptionCommonRequirementsTestFixture : public TS3BackupTestFixture {
    void SetUp(NUnitTest::TTestContext& /* context */) override {
        EnableAllExportAndImportInFeatureFlags();

        AppConfig().MutableFeatureFlags()->SetEnableExternalDataSources(true);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    void EnableAllExportAndImportInFeatureFlags() {
        // Enable all export/import features, even future features
        using namespace google::protobuf;
        NKikimrConfig::TFeatureFlags* featureFlags = AppConfig().MutableFeatureFlags();
        const Reflection* reflection = featureFlags->GetReflection();
        const Descriptor* descriptor = featureFlags->descriptor();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const FieldDescriptor* fieldDescriptor = descriptor->field(i);
            if (fieldDescriptor->name().find("Export") == TProtoStringType::npos && fieldDescriptor->name().find("Import") == TProtoStringType::npos) {
                continue;
            }
            if (fieldDescriptor->type() != FieldDescriptor::TYPE_BOOL) {
                continue;
            }
            reflection->SetBool(featureFlags, fieldDescriptor, true);
        }
    }

protected:
    bool NotEncryptedFileName(const TString& key, const TString& metadataPrefix) {
        return key.EndsWith(".sha256") || key == metadataPrefix + "metadata.json";
    }

    TString ReencryptWithDifferentIV(const TString& source, NBackup::TEncryptionKey& encryptionKey, const std::string& algorithm) {
        auto [content, iv] = NBackup::TEncryptedFileDeserializer::DecryptFullFile(
            encryptionKey,
            TBuffer(source.data(), source.size()));
        auto encrypted = NBackup::TEncryptedFileSerializer::EncryptFullFile(
            TString(algorithm),
            encryptionKey,
            NBackup::TEncryptionIV::Generate(),
            TStringBuf(content.Data(), content.Size()));
        return TString(encrypted.Data(), encrypted.Size());
    }

    void TestCommonEncryptionRequirements(bool useSchemaSecrets, bool isFsBackup = false) {
        using namespace ::fmt::literals;

        THolder<TTempDir> tempDirHolder;
        TString basePath;
        if (isFsBackup) {
            tempDirHolder = MakeHolder<TTempDir>();
            basePath = tempDirHolder->Path();
            Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableFsBackups(true);
        }
        // Create different objects with names that are expected to be hidden (anonymized) in encrypted exports
        // Create two object of each type in order to verify that we don't duplicate IVs
        {
            auto res = YdbQueryClient().ExecuteQuery(
                fmt::format(
                    R"sql(
                        CREATE TABLE `/Root/Anonymized_Dir/Anonymized_Table` (
                            Key Uint32 NOT NULL,
                            Value String NOT NULL,
                            Value2 String NOT NULL,
                            PRIMARY KEY (Key),
                            INDEX `Anonymized_Index` GLOBAL ON (`Value`),
                            INDEX `Anonymized_Index2` GLOBAL ON (`Value2`)
                        )
                        WITH (
                            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2,
                            PARTITION_AT_KEYS = (42)
                        );

                        CREATE TABLE `/Root/Anonymized_Dir/Anonymized_Table2` (
                            Key Uint32 NOT NULL,
                            Value String NOT NULL,
                            Value2 String NOT NULL,
                            PRIMARY KEY (Key),
                            INDEX `Anonymized_Index` GLOBAL ON (`Value`),
                            INDEX `Anonymized_Index2` GLOBAL ON (`Value2`)
                        );

                        ALTER TABLE `/Root/Anonymized_Dir/Anonymized_Table`
                            ADD CHANGEFEED Anonymized_Changefeed WITH (format="JSON", mode="UPDATES");

                        ALTER TABLE `/Root/Anonymized_Dir/Anonymized_Table`
                            ADD CHANGEFEED Anonymized_Changefeed2 WITH (format="JSON", mode="UPDATES");

                        CREATE VIEW `/Root/Anonymized_Dir/Anonymized_View`
                            WITH (security_invoker = TRUE) AS
                                SELECT Value FROM `/Root/Anonymized_Dir/Anonymized_Table`
                                    WHERE Key = 42;

                        CREATE VIEW `/Root/Anonymized_Dir/Anonymized_View2`
                            WITH (security_invoker = TRUE) AS
                                SELECT Value FROM `/Root/Anonymized_Dir/Anonymized_Table`
                                    WHERE Key = 42;

                        CREATE TOPIC `/Root/Anonymized_Dir/Anonymized_Topic` (
                            CONSUMER Anonymized_Consumer,
                            CONSUMER Anonymized_Consumer2
                        );

                        CREATE TOPIC `/Root/Anonymized_Dir/Anonymized_Topic2` (
                            CONSUMER Anonymized_Consumer,
                            CONSUMER Anonymized_Consumer2
                        );

                        CREATE USER anonymizeduser;
                        CREATE USER anonymizeduser2;

                        CREATE GROUP anonymizedgroup WITH USER anonymizeduser, anonymizeduser2;
                        CREATE GROUP anonymizedgroup2 WITH USER anonymizeduser, anonymizeduser2;

                        CREATE OBJECT id (TYPE SECRET) WITH (value=`test_id`);
                        CREATE OBJECT key (TYPE SECRET) WITH (value=`test_key`);
                        CREATE SECRET id WITH (value="test_id");
                        CREATE SECRET key WITH (value="test_key");
                        CREATE EXTERNAL DATA SOURCE `/Root/Anonymized_Dir/Anonymized_DataSource` WITH (
                            SOURCE_TYPE="ObjectStorage",
                            LOCATION="localhost:42",
                            AUTH_METHOD="AWS",
                            {secret_param_name_1}="id",
                            {secret_param_name_2}="key",
                            AWS_REGION="test-central-1"
                        );

                        CREATE OBJECT id2 (TYPE SECRET) WITH (value=`test_id`);
                        CREATE OBJECT key2 (TYPE SECRET) WITH (value=`test_key`);
                        CREATE SECRET id2 WITH (value="test_id");
                        CREATE SECRET key2 WITH (value="test_key");
                        CREATE EXTERNAL DATA SOURCE `/Root/Anonymized_Dir/Anonymized_DataSource2` WITH (
                            SOURCE_TYPE="ObjectStorage",
                            LOCATION="localhost:42",
                            AUTH_METHOD="AWS",
                            {secret_param_name_1}="id2",
                            {secret_param_name_2}="key2",
                            AWS_REGION="test-central-2"
                        );

                        CREATE EXTERNAL TABLE `/Root/Anonymized_Dir/Anonymized_ExternalTable` (
                            Key Uint64,
                            Value String
                        ) WITH (
                            DATA_SOURCE="/Root/Anonymized_Dir/Anonymized_DataSource",
                            LOCATION="/"
                        );

                        CREATE EXTERNAL TABLE `/Root/Anonymized_Dir/Anonymized_ExternalTable2` (
                            Key Uint64,
                            Value String
                        ) WITH (
                            DATA_SOURCE="/Root/Anonymized_Dir/Anonymized_DataSource2",
                            LOCATION="/"
                        );
                    )sql",
                    "secret_param_name_1"_a = useSchemaSecrets ? "AWS_ACCESS_KEY_ID_SECRET_PATH" : "AWS_ACCESS_KEY_ID_SECRET_NAME",
                    "secret_param_name_2"_a = useSchemaSecrets ? "AWS_SECRET_ACCESS_KEY_SECRET_PATH" : "AWS_SECRET_ACCESS_KEY_SECRET_NAME"
                ),
                NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

            auto res2 = YdbQueryClient().ExecuteQuery(R"sql(
                UPSERT INTO `/Root/Anonymized_Dir/Anonymized_Table`
                (Key, Value, Value2)
                VALUES
                    (1, "100", "100"),
                    (100, "1", "001");
            )sql", NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(res2.IsSuccess(), res2.GetIssues().ToString());

            auto createRateLimiterResource = [&](const TString& coordinationNodePath, const TString& path) {
                auto rateLimiterRes = YdbRateLimiterClient().CreateResource(
                    coordinationNodePath,
                    path,
                    NYdb::NRateLimiter::TCreateResourceSettings()
                        .MaxUnitsPerSecond(42.0)
                ).GetValueSync();
                UNIT_ASSERT_C(rateLimiterRes.IsSuccess(), rateLimiterRes.GetIssues().ToString());
            };

            auto createKesus = [&](const TString& path) {
                auto nodeRes = YdbCoordinationClient().CreateNode(path).GetValueSync();
                UNIT_ASSERT_C(nodeRes.IsSuccess(), nodeRes.GetIssues().ToString());

                createRateLimiterResource(path, "Anonymized_Dir");
                createRateLimiterResource(path, "Anonymized_Dir/Anonymized_Resource");
                createRateLimiterResource(path, "Anonymized_Dir/Anonymized_Resource2");
            };

            createKesus("/Root/Anonymized_Dir/Anonymized_Kesus");
            createKesus("/Root/Anonymized_Dir/Anonymized_Kesus2");
        }

        // Create recursive export
        if (isFsBackup) {
            NExport::TExportToFsSettings settings;
            settings
                .BasePath(basePath)
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");

            auto res = YdbExportClient().ExportToFs(settings).GetValueSync();
            WaitOpSuccess(res);
        } else {
            NExport::TExportToS3Settings settings = MakeExportSettings("", "Prefix");
            settings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");

            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            WaitOpSuccess(res);
        }

        NBackup::TEncryptionKey encryptionKey("Cool random key!");
        THashSet<TString> ivs;
        THashSet<TString> allKeyNames;

        THolder<NKikimr::NWrappers::NTestHelpers::TFsMock> fsMockOwner;
        if (isFsBackup) {
            fsMockOwner = MakeHolder<NKikimr::NWrappers::NTestHelpers::TFsMock>(basePath);
            fsMockOwner->Refresh();
        }
        auto& mock = isFsBackup
            ? static_cast<NKikimr::NWrappers::NTestHelpers::TBackupMock&>(*fsMockOwner)
            : static_cast<NKikimr::NWrappers::NTestHelpers::TBackupMock&>(S3Mock());

        const TString metadataPrefix = isFsBackup ? "" : "/test_bucket/Prefix/";

        Cerr << "Export files:\n";
        for (const auto& [key, content] : mock.GetData()) {
            Cerr << key << Endl;

            if (NotEncryptedFileName(key, metadataPrefix)) {
                continue;
            }

            allKeyNames.insert(key);

            // Check that files are encrypted
            UNIT_ASSERT_C(key.EndsWith(".enc"), key);

            // Check that we can decrypt content with our key (== it is really encrypted with our key)
            TBuffer decryptedData;
            NBackup::TEncryptionIV iv;
            UNIT_ASSERT_NO_EXCEPTION_C(std::tie(decryptedData, iv) = NBackup::TEncryptedFileDeserializer::DecryptFullFile(
                encryptionKey,
                TBuffer(content.data(), content.size())
            ), key);

            // All ivs are unique
            UNIT_ASSERT_C(ivs.insert(iv.GetBinaryString()).second, key);

            // Encrypted export must not show objects real names
            UNIT_ASSERT_C(key.find("Anonymized") == TString::npos, key);
            UNIT_ASSERT_C(key.find("anonymized") == TString::npos, key); // user/group
        }

        if (isFsBackup) {
            // TODO: FS import with encryption and recursive mode is not yet fully implemented
            // Skip import tests for FS backups for now
            Cerr << "Skipping import tests for FS backup (not yet fully implemented)" << Endl;
            return;
        }

        NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/Restored");
        importSettings
            .SymmetricKey("Cool random key!");

        size_t importIndex = 0;
        auto copySettings = [&]() {
            NYdb::NImport::TImportFromS3Settings settings = importSettings;
            settings.DestinationPath(TStringBuilder() << "/Root/Prefix_" << importIndex++);
            return settings;
        };

        // Check that import is initially OK
        auto res = YdbImportClient().ImportFromS3(copySettings()).GetValueSync();
        WaitOpSuccess(res);
        ForgetOp(res);

        auto checkImportFails = [&](const TString& comments) {
            auto res = YdbImportClient().ImportFromS3(copySettings()).GetValueSync();
            WaitOpStatus(res, NYdb::EStatus::CANCELLED, comments);
            ForgetOp(res);
        };

        // Check that if we remove any key, import will fail,
        // if we modify the file, import will fail,
        // if we rewrite the file with another IV, import will fail.
        for (const TString& key : allKeyNames) {
            const auto fileIt = S3Mock().GetData().find(key);
            UNIT_ASSERT_C(fileIt != S3Mock().GetData().end(), "No file: " << key);

            const TString sourceValue = fileIt->second;

            Y_DEFER {
                S3Mock().GetData()[key] = sourceValue;
            };

            // Remove one file from export.
            // In case of encrypted backup it must cause error,
            // because no one should be able not modify export files,
            // in particular, remove an export part (==file).
            S3Mock().GetData().erase(key);
            checkImportFails(TStringBuilder() << "Remove key " << key);

            // Change IV (reencrypt with different, not expected, IV)
            S3Mock().GetData()[key] = ReencryptWithDifferentIV(sourceValue, encryptionKey, NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM);
            checkImportFails(TStringBuilder() << "Change IV of " << key);
        }
    }
};

Y_UNIT_TEST_SUITE_F(CommonEncryptionRequirementsTest, TBackupEncryptionCommonRequirementsTestFixture) {
    Y_UNIT_TEST(CommonEncryptionRequirements) {
        TestCommonEncryptionRequirements(/* useSchemaSecrets */ false);
    }

    Y_UNIT_TEST(CommonEncryptionRequirementsWithSchemaSecrets) {
        TestCommonEncryptionRequirements(/* useSchemaSecrets */ true);
    }

    Y_UNIT_TEST(CommonEncryptionRequirementsFs) {
        TestCommonEncryptionRequirements(/* useSchemaSecrets */ false, /* isFsBackup */ true);
    }

    Y_UNIT_TEST(CommonEncryptionRequirementsFsWithSchemaSecrets) {
        TestCommonEncryptionRequirements(/* useSchemaSecrets */ true, /* isFsBackup */ true);
    }
}

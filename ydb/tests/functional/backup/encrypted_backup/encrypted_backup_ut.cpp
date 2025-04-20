#include <ydb/tests/functional/backup/helpers/backup_test_fixture.h>

#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <library/cpp/testing/unittest/registar.h>

#include <fmt/format.h>

#include <vector>

using namespace NYdb;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE_F(EncryptedBackupTest, TBackupTestFixture)
{
    Y_UNIT_TEST(ExportParamsValidation)
    {
        {
            auto res = YdbQueryClient().ExecuteQuery(R"sql(
                CREATE TABLE `/local/ExportParamsValidation/dir1/Table1` (
                    Key Uint32 NOT NULL,
                    PRIMARY KEY (Key)
                );
            )sql", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        NExport::TExportToS3Settings baseSettings;
        baseSettings.Endpoint(S3Endpoint());
        baseSettings.Bucket("bucket-name");
        baseSettings.AccessKey("minio");
        baseSettings.SecretKey("minio123");

        { // bad source path
            NExport::TExportToS3Settings settings = baseSettings;
            settings.SourcePath("unknown").DestinationPrefix("dest");
            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            UNIT_ASSERT(!res.Status().IsSuccess());
            UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::SCHEME_ERROR, res.Status().GetIssues().ToString());

            // Fix
            {
                NExport::TExportToS3Settings fixSettings = settings;
                fixSettings.SourcePath("/local/");
                auto res = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
                UNIT_ASSERT_C(res.Status().IsSuccess(), res.Status().GetIssues().ToString());
            }
        }

        { // no destination
            NExport::TExportToS3Settings settings = baseSettings;
            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            UNIT_ASSERT(!res.Status().IsSuccess());
            UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());

            // Fix
            {
                NExport::TExportToS3Settings fixSettings = settings;
                fixSettings.DestinationPrefix("dest");
                auto res = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
                UNIT_ASSERT_C(res.Status().IsSuccess(), res.Status().GetIssues().ToString());
            }
        }

        { // no destination
            NExport::TExportToS3Settings settings = baseSettings;
            settings.AppendItem({"/local/ExportParamsValidation/dir1/Table1", ""});
            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            UNIT_ASSERT(!res.Status().IsSuccess());
            UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());

            // Fix
            {
                NExport::TExportToS3Settings fixSettings = settings;
                fixSettings.Item_[0].Dst = "aaa";
                auto res2 = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
                UNIT_ASSERT_C(res2.Status().IsSuccess(), res2.Status().GetIssues().ToString());
            }

            // Fix 2
            {
                NExport::TExportToS3Settings fixSettings = settings;
                fixSettings.DestinationPrefix("aaa");
                auto res3 = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
                UNIT_ASSERT_C(res3.Status().IsSuccess(), res3.Status().GetIssues().ToString());
            }
        }

        { // bad encryption params: has encryption, but no common destination
            NExport::TExportToS3Settings settings = baseSettings;
            settings.AppendItem({"/local/ExportParamsValidation/dir1/Table1", "dest"});
            settings.SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            UNIT_ASSERT(!res.Status().IsSuccess());
            UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());

            // Fix
            {
                NExport::TExportToS3Settings fixSettings = settings;
                fixSettings.DestinationPrefix("aaa");
                auto res3 = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
                UNIT_ASSERT_C(res3.Status().IsSuccess(), res3.Status().GetIssues().ToString());
            }
        }

        { // bad encryption params: incorrect key length
            NExport::TExportToS3Settings settings = baseSettings;
            settings.DestinationPrefix("encrypted_export");
            settings.SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "123");
            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            UNIT_ASSERT(!res.Status().IsSuccess());
            UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, res.Status().GetIssues().ToString());

            // Fix
            {
                NExport::TExportToS3Settings fixSettings = settings;
                fixSettings.SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
                auto res3 = YdbExportClient().ExportToS3(fixSettings).GetValueSync();
                UNIT_ASSERT_C(res3.Status().IsSuccess(), res3.Status().GetIssues().ToString());
            }
        }
    }

    Y_UNIT_TEST(EncryptedExportAndImport)
    {
        {
            auto res = YdbQueryClient().ExecuteQuery(R"sql(
                CREATE TABLE `/local/EncryptedExportAndImport/dir1/dir2/EncryptedExportAndImportTable` (
                    Key Uint32 NOT NULL,
                    PRIMARY KEY (Key)
                );
            )sql", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const TString bucketName = "EncryptedExportBucket";
        CreateBucket(bucketName);

        {
            NExport::TExportToS3Settings exportSettings;
            exportSettings
                .Endpoint(S3Endpoint())
                .Bucket(bucketName)
                .Scheme(NYdb::ES3Scheme::HTTP)
                .AccessKey("minio")
                .SecretKey("minio123")
                .SourcePath("/local/EncryptedExportAndImport/dir1")
                .DestinationPrefix("EncryptedExport")
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            std::vector<TString> keys = NTestUtils::GetObjectKeys(bucketName, S3Client(), "EncryptedExport");
            std::sort(keys.begin(), keys.end());
            std::vector<TString> expectedKeys = {
                "EncryptedExport/001/data_00.csv.enc",
                "EncryptedExport/001/metadata.json.enc",
                "EncryptedExport/001/scheme.pb.enc",
                "EncryptedExport/SchemaMapping/mapping.json.enc",
                "EncryptedExport/SchemaMapping/metadata.json.enc",
                "EncryptedExport/metadata.json",
            };
            UNIT_ASSERT_VALUES_EQUAL(keys, expectedKeys);
        }
    }
}

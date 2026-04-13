#pragma once

#include "backup_test_base.h"

#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/scope.h>

class TS3BackupTestFixture : public TBackupTestBaseFixture {
public:
    NYdb::NExport::TExportToS3Settings MakeExportSettings(const TString& sourcePath, const TString& destinationPrefix) {
        NYdb::NExport::TExportToS3Settings exportSettings;
        exportSettings
            .Endpoint(TStringBuilder() << "localhost:" << S3Port())
            .Bucket("test_bucket")
            .Scheme(NYdb::ES3Scheme::HTTP)
            .AccessKey("test_key")
            .SecretKey("test_secret");
        if (destinationPrefix) {
            exportSettings.DestinationPrefix(destinationPrefix);
        }
        if (sourcePath) {
            exportSettings.SourcePath(sourcePath);
        }
        return exportSettings;
    }

    NYdb::NImport::TImportFromS3Settings MakeImportSettings(const TString& sourcePrefix, const TString& destinationPath) {
        NYdb::NImport::TImportFromS3Settings importSettings;
        importSettings
            .Endpoint(TStringBuilder() << "localhost:" << S3Port())
            .Bucket("test_bucket")
            .Scheme(NYdb::ES3Scheme::HTTP)
            .AccessKey("test_key")
            .SecretKey("test_secret");
        if (sourcePrefix) {
            importSettings.SourcePrefix(sourcePrefix);
        }
        if (destinationPath) {
            importSettings.DestinationPath(destinationPath);
        }
        return importSettings;
    }

    void ValidateS3FileList(const TSet<TString>& paths, const TString& prefix = {}) {
        TSet<TString> keys;
        for (const auto& [key, _] : S3Mock().GetData()) {
            if (!prefix || key.StartsWith(prefix)) {
                keys.insert(key);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(keys, paths);
    }

protected:
    TS3BackupTestFixture() = default;

    NKikimr::NWrappers::NTestHelpers::TS3Mock& S3Mock() {
        if (!S3Mock_) {
            S3Port_ = Server().GetPortManager().GetPort();
            S3Mock_.ConstructInPlace(NKikimr::NWrappers::NTestHelpers::TS3Mock::TSettings(S3Port_));
            UNIT_ASSERT_C(S3Mock_->Start(), S3Mock_->GetError());
        }
        return *S3Mock_;
    }

    ui16 S3Port() {
        S3Mock();
        return S3Port_;
    }


    template <class TResponseType>
    void ForgetOp(const TResponseType& res) {
        auto result = YdbOperationClient().Forget(res.Id()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "Status: " << result.GetStatus() << ". Issues: " << result.GetIssues().ToString());
    }

    NYdb::NImport::TListObjectsInS3ExportSettings MakeListObjectsInS3ExportSettings(const TString& prefix) {
        NYdb::NImport::TListObjectsInS3ExportSettings listSettings;
        listSettings
            .Endpoint(TStringBuilder() << "localhost:" << S3Port())
            .Bucket("test_bucket")
            .Scheme(NYdb::ES3Scheme::HTTP)
            .AccessKey("test_key")
            .SecretKey("test_secret");
        if (prefix) {
            listSettings.Prefix(prefix);
        }
        return listSettings;
    }

    void ValidateListObjectInS3Export(const TSet<std::pair<TString /*prefix*/, TString /*path*/>>& paths, const NYdb::NImport::TListObjectsInS3ExportSettings& listSettings) {
        auto res = YdbImportClient().ListObjectsInS3Export(listSettings).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), "Status: " << res.GetStatus() << ". Issues: " << res.GetIssues().ToString());

        TSet<std::pair<TString, TString>> pathsInResponse;
        for (const auto& item : res.GetItems()) {
            bool inserted = pathsInResponse.emplace(item.Prefix, item.Path).second;
            UNIT_ASSERT_C(inserted, "Duplicate item: {" << item.Prefix << ", " << item.Path << "}. Listing result: " << res);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(pathsInResponse, paths, "Listing result: " << res);
    }

    void ValidateListObjectInS3Export(const TSet<std::pair<TString /*prefix*/, TString /*path*/>>& paths, const TString& exportPrefix) {
        ValidateListObjectInS3Export(paths, MakeListObjectsInS3ExportSettings(exportPrefix));
    }

    void ValidateListObjectPathsInS3Export(const TSet<TString>& paths, const NYdb::NImport::TListObjectsInS3ExportSettings& listSettings) {
        auto res = YdbImportClient().ListObjectsInS3Export(listSettings).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), "Status: " << res.GetStatus() << ". Issues: " << res.GetIssues().ToString());

        TSet<TString> pathsInResponse;
        for (const auto& item : res.GetItems()) {
            bool inserted = pathsInResponse.emplace(item.Path).second;
            UNIT_ASSERT_C(inserted, "Duplicate item: {" << item.Prefix << ", " << item.Path << "}. Listing result: " << res);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(pathsInResponse, paths, "Listing result: " << res);
    }

    void ValidateListObjectPathsInS3Export(const TSet<TString>& paths, const TString& exportPrefix) {
        ValidateListObjectPathsInS3Export(paths, MakeListObjectsInS3ExportSettings(exportPrefix));
    }

    TString DebugListDir(const TString& path) { // Debug listing for specified dir
        auto res = YdbSchemeClient().ListDirectory(path).GetValueSync();
        TStringBuilder l;
        if (res.IsSuccess()) {
            for (const auto& entry : res.GetChildren()) {
                if (l) {
                    l << ", ";
                }
                l << "\"" << entry.Name << "\"";
            }
        } else {
            l << "List dir \"" << path << "\" failed: " << res.GetIssues().ToOneLineString();
        }
        return l;
    }

    static TString ModifyHexEncodedString(TString value) {
        UNIT_ASSERT_GT(value.size(), 0);
        char c = value.front();
        if (c == '9' || c == 'f' || c == 'F') {
            --c;
        } else {
            ++c;
        }
        value[0] = c;
        return value;
    }

    void ModifyChecksumAndCheckThatImportFails(const TString& checksumFile, const NYdb::NImport::TImportFromS3Settings& importSettings) {
        const auto checksumFileIt = S3Mock().GetData().find(checksumFile);
        UNIT_ASSERT_C(checksumFileIt != S3Mock().GetData().end(), "No checksum file: " << checksumFile);

        // Automatic return to the previous state
        const TString checksumValue = checksumFileIt->second;
        Y_DEFER {
            S3Mock().GetData()[checksumFile] = checksumValue;
        };

        checksumFileIt->second = ModifyHexEncodedString(checksumValue);

        auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
        WaitOpStatus(res, NYdb::EStatus::CANCELLED);
    }

    void ModifyChecksumAndCheckThatImportFails(const std::initializer_list<TString>& checksumFiles, const NYdb::NImport::TImportFromS3Settings& importSettings) {
        auto copySettings = [&]() {
            NYdb::NImport::TImportFromS3Settings settings = importSettings;
            settings.DestinationPath(TStringBuilder() << "/Root/Prefix_" << RestoreAttempt++);
            return settings;
        };

        // Check that settings are OK
        auto res = YdbImportClient().ImportFromS3(copySettings()).GetValueSync();
        WaitOpSuccess(res);

        for (const TString& checksumFile : checksumFiles) {
            ModifyChecksumAndCheckThatImportFails(checksumFile, copySettings());
        }
    }

    void TestSchemeObjectEncryptedExportImport(
        const TString& query,
        const TString& objectName,
        const TSet<TString> s3FileList)
    {
        using namespace NYdb;

        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableChecksumsExport(true);
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnablePermissionsExport(true);

        // Enable all
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableViewExport(true);
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableExternalDataSources(true);
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableReplication(true);

        auto res = YdbQueryClient().ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        {
            NExport::TExportToS3Settings settings = MakeExportSettings("/Root/EncryptedExportAndImport/dir1/dir2/dir3", "Prefix");
            settings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");

            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList(s3FileList);
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/Restored");
            importSettings
                .SymmetricKey("Cool random key!");

            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);
        }

        auto desc = YdbSchemeClient().DescribePath(Sprintf("/Root/Restored/%s", objectName.c_str())).GetValueSync();
        UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
    }

    YDB_SDK_CLIENT(NYdb::NTable::TTableClient, YdbTableClient);
    YDB_SDK_CLIENT(NYdb::NTopic::TTopicClient, YdbTopicClient);
    YDB_SDK_CLIENT(NYdb::NCoordination::TClient, YdbCoordinationClient);
    YDB_SDK_CLIENT(NYdb::NRateLimiter::TRateLimiterClient, YdbRateLimiterClient);

private:
    ui16 S3Port_ = 0;
    TMaybe<NKikimr::NWrappers::NTestHelpers::TS3Mock> S3Mock_;
    size_t RestoreAttempt = 0;
};

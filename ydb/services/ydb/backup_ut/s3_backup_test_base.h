#pragma once

#include "backup_test_base.h"

#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/services/ydb/ydb_common_ut.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/generic/is_in.h>
#include <util/system/env.h>
#include <util/system/sanitizers.h>

class TS3BackupTestFixture : public NUnitTest::TBaseFixture {
public:
    static constexpr TDuration DEFAULT_OPERATION_WAIT_TIME = NSan::PlainOrUnderSanitizer(TDuration::Seconds(60), TDuration::Seconds(300));
    static constexpr TDuration START_OPERATION_WAIT_TIME = NSan::PlainOrUnderSanitizer(TDuration::MilliSeconds(100), TDuration::Seconds(1));
    static constexpr TDuration MAX_OPERATION_WAIT_TIME = NSan::PlainOrUnderSanitizer(TDuration::Seconds(2), TDuration::Seconds(30));

protected:
    TS3BackupTestFixture() {
        AppConfig().MutableFeatureFlags()->SetEnableColumnTablesBackup(true);
    }

    TString YdbConnectionString() {
        return TStringBuilder() << "localhost:" << Server().GetPort() << "/?database=/Root";
    }

    NYdb::TDriverConfig& YdbDriverConfig() {
        if (!DriverConfig) {
            DriverConfig.ConstructInPlace(YdbConnectionString());
        }
        return *DriverConfig;
    }

    NYdb::TDriver& YdbDriver() {
        if (!Driver) {
            Driver.ConstructInPlace(YdbDriverConfig());
        }
        return *Driver;
    }

    YDB_SDK_CLIENT(NYdb::NTable::TTableClient, YdbTableClient);
    YDB_SDK_CLIENT(NYdb::NExport::TExportClient, YdbExportClient);
    YDB_SDK_CLIENT(NYdb::NImport::TImportClient, YdbImportClient);
    YDB_SDK_CLIENT(NYdb::NQuery::TQueryClient, YdbQueryClient);
    YDB_SDK_CLIENT(NYdb::NScheme::TSchemeClient, YdbSchemeClient);
    YDB_SDK_CLIENT(NYdb::NOperation::TOperationClient, YdbOperationClient);
    YDB_SDK_CLIENT(NYdb::NTopic::TTopicClient, YdbTopicClient);
    YDB_SDK_CLIENT(NYdb::NCoordination::TClient, YdbCoordinationClient);
    YDB_SDK_CLIENT(NYdb::NRateLimiter::TRateLimiterClient, YdbRateLimiterClient);

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

    NKikimrConfig::TAppConfig& AppConfig() {
        return AppConfig_;
    }

    NYdb::TKikimrWithGrpcAndRootSchema& Server() {
        if (!Server_) {
            Server_.ConstructInPlace(AppConfig());

            auto& runtime = *Server_->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::EPriority::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::EXPORT, NLog::EPriority::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::IMPORT, NLog::EPriority::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::EPriority::PRI_TRACE);
            runtime.GetAppData().FeatureFlags.SetEnableViewExport(true);
            runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(true);
            runtime.GetAppData().DataShardExportFactory = &DataShardExportFactory;
        }
        return *Server_;
    }

    template<typename TOp>
    void WaitOp(TMaybe<NYdb::TOperation>& op, TDuration timeout = DEFAULT_OPERATION_WAIT_TIME) {
        const TInstant start = TInstant::Now();
        bool ok = false;
        TDuration waitTime = START_OPERATION_WAIT_TIME;
        while (TInstant::Now() - start <= timeout) {
            op = YdbOperationClient().Get<TOp>(op->Id()).GetValueSync();
            if (op->Ready()) {
                ok = true;
                break;
            }
            Sleep(waitTime);
            waitTime *= 1.3;
            waitTime = Min(waitTime, MAX_OPERATION_WAIT_TIME);
        }
        UNIT_ASSERT_C(ok, "Unable to wait completion of operation");
    }

    template <class TResponseType>
    TMaybe<NYdb::TOperation> WaitOpSuccess(const TResponseType& res, const TString& comments = {}, TDuration timeout = DEFAULT_OPERATION_WAIT_TIME) {
        return WaitOpStatus<TResponseType>(res, NYdb::EStatus::SUCCESS, comments, timeout);
    }

    template <class TResponseType>
    TMaybe<NYdb::TOperation> WaitOpStatus(const TResponseType& res, const std::vector<NYdb::EStatus>& status, const TString& comments = {}, TDuration timeout = DEFAULT_OPERATION_WAIT_TIME) {
        if (res.Ready()) {
            UNIT_ASSERT_C(IsIn(status, res.Status().GetStatus()), comments << ". Status: " << res.Status().GetStatus() << ". Issues: " << res.Status().GetIssues().ToString());
            return res;
        } else {
            TMaybe<NYdb::TOperation> op = res;
            WaitOp<TResponseType>(op, timeout);
            UNIT_ASSERT_C(IsIn(status, op->Status().GetStatus()), comments << ". Status: " << op->Status().GetStatus() << ". Issues: " << op->Status().GetIssues().ToString());
            return op;
        }
    }

    template <class TResponseType>
    TMaybe<NYdb::TOperation> WaitOpStatus(const TResponseType& res, NYdb::EStatus status, const TString& comments = {}, TDuration timeout = DEFAULT_OPERATION_WAIT_TIME) {
        std::vector<NYdb::EStatus> statuses(1, status);
        return WaitOpStatus(res, statuses, comments, timeout);
    }

    template <class TResponseType>
    void ForgetOp(const TResponseType& res) {
        auto result = YdbOperationClient().Forget(res.Id()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "Status: " << result.GetStatus() << ". Issues: " << result.GetIssues().ToString());
    }

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

    void ValidateS3FileList(const TSet<TString>& paths, const TString& prefix = {}) {
        TSet<TString> keys;
        for (const auto& [key, _] : S3Mock().GetData()) {
            if (!prefix || key.StartsWith(prefix)) {
                keys.insert(key);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(keys, paths);
    }

    struct TEntryPath {
        TString Path;
        NYdb::NScheme::ESchemeEntryType Type;

        TEntryPath(const TString& path, NYdb::NScheme::ESchemeEntryType type)
            : Path(path)
            , Type(type)
        {}

        static TEntryPath TablePath(const TString& path, bool isColumnTable) {
            return TEntryPath(path, isColumnTable ? NYdb::NScheme::ESchemeEntryType::ColumnTable : NYdb::NScheme::ESchemeEntryType::Table);
        }
    };

    void ValidateHasYdbPaths(const std::vector<TEntryPath>& paths) {
        for (const auto& item : paths) {
            auto res = YdbSchemeClient().DescribePath(item.Path).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), "Describe path \"" << item.Path << "\" failed: " << res.GetIssues().ToString());
            UNIT_ASSERT_C(res.GetEntry().Type == item.Type, "Path " << item.Path << " has wrong type. Expected: " << item.Type << ", actual: " << res.GetEntry().Type);
        }
    }
    
    void ValidateHasYdbTables(const std::vector<TString>& paths) {
        for (const TString& path : paths) {
            auto res = YdbSchemeClient().DescribePath(path).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), "Describe path \"" << path << "\" failed: " << res.GetIssues().ToString());
            UNIT_ASSERT_C(res.GetEntry().Type == NYdb::NScheme::ESchemeEntryType::Table, "Path " << path << " is not a table. Path type: " << static_cast<int>(res.GetEntry().Type));
        }
    }

    void ValidateDoesNotHaveYdbTables(const std::vector<TString>& paths) {
        for (const TString& path : paths) {
            auto res = YdbSchemeClient().DescribePath(path).GetValueSync();
            UNIT_ASSERT_C(!res.IsSuccess(), "Describe path \"" << path << "\" succeeded, but test expects that there is no such path");
            UNIT_ASSERT_C(res.GetStatus() == NYdb::EStatus::SCHEME_ERROR, "Wrong status for describe path \"" << path << "\": " << res.GetStatus());
        }
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

private:
    TDataShardExportFactory DataShardExportFactory;
    NKikimrConfig::TAppConfig AppConfig_;
    TMaybe<NYdb::TKikimrWithGrpcAndRootSchema> Server_;
    ui16 S3Port_ = 0;
    TMaybe<NKikimr::NWrappers::NTestHelpers::TS3Mock> S3Mock_;
    TMaybe<NYdb::TDriverConfig> DriverConfig;
    TMaybe<NYdb::TDriver> Driver;
    size_t RestoreAttempt = 0;
};

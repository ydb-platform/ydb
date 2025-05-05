#pragma once
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/services/ydb/ydb_common_ut.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>

class TS3BackupTestFixture : public NUnitTest::TBaseFixture {
protected:
    TS3BackupTestFixture()
        : S3Port(Server.GetPortManager().GetPort())
        , S3Mock(NKikimr::NWrappers::NTestHelpers::TS3Mock::TSettings(S3Port))
    {
        UNIT_ASSERT_C(S3Mock.Start(), S3Mock.GetError());
        auto& runtime = *Server.GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::EPriority::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NLog::EPriority::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::IMPORT, NLog::EPriority::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::EPriority::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableViews(true);
        runtime.GetAppData().FeatureFlags.SetEnableViewExport(true);
        runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(true);
        runtime.GetAppData().DataShardExportFactory = &DataShardExportFactory;
    }

    TString YdbConnectionString() const {
        return TStringBuilder() << "localhost:" << Server.GetPort() << "/?database=/Root";
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

#define YDB_SDK_CLIENT(type, funcName)                               \
    protected:                                                       \
    TMaybe<type> Y_CAT(funcName, Instance);                          \
    public:                                                          \
    type& funcName() {                                               \
        if (!Y_CAT(funcName, Instance)) {                            \
            Y_CAT(funcName, Instance).ConstructInPlace(YdbDriver()); \
        }                                                            \
        return *Y_CAT(funcName, Instance);                           \
    }                                                                \
    /**/

    YDB_SDK_CLIENT(NYdb::NTable::TTableClient, YdbTableClient);
    YDB_SDK_CLIENT(NYdb::NExport::TExportClient, YdbExportClient);
    YDB_SDK_CLIENT(NYdb::NImport::TImportClient, YdbImportClient);
    YDB_SDK_CLIENT(NYdb::NQuery::TQueryClient, YdbQueryClient);
    YDB_SDK_CLIENT(NYdb::NScheme::TSchemeClient, YdbSchemeClient);
    YDB_SDK_CLIENT(NYdb::NOperation::TOperationClient, YdbOperationClient);

#undef YDB_SDK_CLIENT

    template<typename TOp>
    void WaitOp(TMaybe<NYdb::TOperation>& op) {
        int attempt = 200;
        while (--attempt) {
            op = YdbOperationClient().Get<TOp>(op->Id()).GetValueSync();
            if (op->Ready()) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(attempt, "Unable to wait completion of operation");
    }

    template <class TResponseType>
    TMaybe<NYdb::TOperation> WaitOpSuccess(const TResponseType& res) {
        return WaitOpStatus<TResponseType>(res, NYdb::EStatus::SUCCESS);
    }

    template <class TResponseType>
    TMaybe<NYdb::TOperation> WaitOpStatus(const TResponseType& res, NYdb::EStatus status) {
        if (res.Ready()) {
            UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), status, "Status: " << res.Status().GetStatus() << ". Issues: " << res.Status().GetIssues().ToString());
            return res;
        } else {
            TMaybe<NYdb::TOperation> op = res;
            WaitOp<TResponseType>(op);
            UNIT_ASSERT_VALUES_EQUAL_C(op->Status().GetStatus(), status, "Status: " << op->Status().GetStatus() << ". Issues: " << op->Status().GetIssues().ToString());
            return op;
        }
    }

    NYdb::NExport::TExportToS3Settings MakeExportSettings(const TString& sourcePath, const TString& destinationPrefix) {
        NYdb::NExport::TExportToS3Settings exportSettings;
        exportSettings
            .Endpoint(TStringBuilder() << "localhost:" << S3Port)
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
            .Endpoint(TStringBuilder() << "localhost:" << S3Port)
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
        for (const auto& [key, _] : S3Mock.GetData()) {
            if (!prefix || key.StartsWith(prefix)) {
                keys.insert(key);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(keys, paths);
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

protected:
    TDataShardExportFactory DataShardExportFactory;
    NYdb::TKikimrWithGrpcAndRootSchema Server;
    const ui16 S3Port;
    NKikimr::NWrappers::NTestHelpers::TS3Mock S3Mock;
    TMaybe<NYdb::TDriverConfig> DriverConfig;
    TMaybe<NYdb::TDriver> Driver;
};

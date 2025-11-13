#include "backup_test_base.h"

#include <ydb/services/ydb/ydb_common_ut.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/system/sanitizers.h>

using namespace NYdb;

class TFsBackupParamsValidationTestFixture : public NUnitTest::TBaseFixture {
public:
    static constexpr TDuration DEFAULT_OPERATION_WAIT_TIME = NSan::PlainOrUnderSanitizer(
        TDuration::Seconds(60), TDuration::Seconds(300));

    TFsBackupParamsValidationTestFixture() = default;

    void SetUp(NUnitTest::TTestContext& /* context */) override {
        auto res = YdbQueryClient().ExecuteQuery(R"sql(
            CREATE TABLE `/Root/FsExportParamsValidation/dir1/Table1` (
                Key Uint32 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );
        )sql", NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        res = YdbQueryClient().ExecuteQuery(R"sql(
            UPSERT INTO `/Root/FsExportParamsValidation/dir1/Table1` (Key, Value)
            VALUES
                (1u, "value1"),
                (2u, "value2"),
                (3u, "value3");
        )sql", NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

protected:
    NYdb::TKikimrWithGrpcAndRootSchema& Server() {
        if (!Server_) {
            Server_.ConstructInPlace();

            auto& runtime = *Server_->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::EPriority::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::EXPORT, NLog::EPriority::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::IMPORT, NLog::EPriority::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::EPriority::PRI_TRACE);
        }
        return *Server_;
    }

    NYdb::TDriver& YdbDriver() {
        if (!Driver) {
            TDriverConfig config;
            config.SetEndpoint(TStringBuilder() << "localhost:" << Server().GetPort());
            config.SetDatabase("/Root");
            Driver.ConstructInPlace(config);
        }
        return *Driver;
    }

    YDB_SDK_CLIENT(NYdb::NExport::TExportClient, YdbExportClient);
    YDB_SDK_CLIENT(NYdb::NImport::TImportClient, YdbImportClient);
    YDB_SDK_CLIENT(NYdb::NQuery::TQueryClient, YdbQueryClient);
    YDB_SDK_CLIENT(NYdb::NOperation::TOperationClient, YdbOperationClient);

    template <class TResponseType>
    void WaitOpSuccess(const TResponseType& res, const TString& comments = {}, 
                      TDuration timeout = DEFAULT_OPERATION_WAIT_TIME) {
        UNIT_ASSERT_C(res.Status().IsSuccess() || !res.Ready(), 
            comments << ". Status: " << res.Status().GetStatus() 
            << ". Issues: " << res.Status().GetIssues().ToString());
        
        if (res.Ready()) {
            return;
        }

        const TInstant start = TInstant::Now();
        TMaybe<NYdb::TOperation> op = res;
        TDuration waitTime = TDuration::MilliSeconds(100);
        
        while (TInstant::Now() - start <= timeout) {
            op = YdbOperationClient().Get<TResponseType>(op->Id()).GetValueSync();
            if (op->Ready()) {
                UNIT_ASSERT_C(op->Status().IsSuccess(), 
                    comments << ". Status: " << op->Status().GetStatus() 
                    << ". Issues: " << op->Status().GetIssues().ToString());
                return;
            }
            Sleep(waitTime);
            waitTime = Min(waitTime * 1.3, TDuration::Seconds(2));
        }
        
        UNIT_ASSERT_C(false, "Operation timeout: " << comments);
    }

    TTempDir& TempDir() {
        if (!TempDir_) {
            TempDir_.ConstructInPlace();
        }
        return *TempDir_;
    }

    NExport::TExportToFsSettings MakeExportSettings(const TString& basePath) {
        NExport::TExportToFsSettings settings;
        if (basePath) {
            settings.BasePath(basePath);
        }
        return settings;
    }

    NImport::TImportFromFsSettings MakeImportSettings(const TString& basePath) {
        NImport::TImportFromFsSettings settings;
        if (basePath) {
            settings.BasePath(basePath);
        }
        return settings;
    }

private:
    TMaybe<NYdb::TKikimrWithGrpcAndRootSchema> Server_;
    TMaybe<NYdb::TDriver> Driver;
    TMaybe<TTempDir> TempDir_;
};

Y_UNIT_TEST_SUITE_F(FsBackupParamsValidationTest, TFsBackupParamsValidationTestFixture)
{
    Y_UNIT_TEST(NoBasePath) {
        // Test that base_path is required
        NExport::TExportToFsSettings settings = MakeExportSettings("");
        
        auto res = YdbExportClient().ExportToFs(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), 
            "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, 
            res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "base_path is required but not set",
            res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(RelativeBasePath) {
        // Test that base_path must be absolute
        NExport::TExportToFsSettings settings = MakeExportSettings("");
        settings.BasePath("relative/path");

        auto res = YdbExportClient().ExportToFs(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), 
            "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, 
            res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "base_path must be an absolute path",
            res.Status().GetIssues().ToString());

        //TODO(st-shchetinin): Uncomment after supporting the entire export pipeline in FS
        // Fix: use absolute path
        {
            // NExport::TExportToFsSettings fixSettings = settings;
            // fixSettings.BasePath(TString(TempDir().Path()));
            // auto res = YdbExportClient().ExportToFs(fixSettings).GetValueSync();
            // WaitOpSuccess(res, "Export with absolute base_path should succeed");
        }
    }

    Y_UNIT_TEST(InvalidCompression) {
        // Test that compression codec must be valid
        NExport::TExportToFsSettings settings = MakeExportSettings(TString(TempDir().Path()));
        settings.Compression("invalid-codec");

        auto res = YdbExportClient().ExportToFs(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), 
            "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::UNSUPPORTED, 
            res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "Unsupported compression codec",
            res.Status().GetIssues().ToString());

        //TODO(st-shchetinin): Uncomment after supporting the entire export pipeline in FS
        // Fix: use valid codec
        {
            // NExport::TExportToFsSettings fixSettings = settings;
            // fixSettings.Compression("zstd");
            // auto res = YdbExportClient().ExportToFs(fixSettings).GetValueSync();
            // WaitOpSuccess(res, "Export with valid compression should succeed");
        }

        // Fix 2: use zstd with level
        {
            // NExport::TExportToFsSettings fixSettings = settings;
            // fixSettings.Compression("zstd-6");
            // auto res = YdbExportClient().ExportToFs(fixSettings).GetValueSync();
            // WaitOpSuccess(res, "Export with zstd-6 should succeed");
        }
    }

    Y_UNIT_TEST(InvalidCompressionLevel) {
        // Test that compression level must be in valid range [1, 22]
        NExport::TExportToFsSettings settings = MakeExportSettings(TString(TempDir().Path()));
        
        // Level too high
        settings.Compression("zstd-100");
        auto res = YdbExportClient().ExportToFs(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), 
            "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, 
            res.Status().GetIssues().ToString());

        // Level = 0
        settings.Compression("zstd-0");
        res = YdbExportClient().ExportToFs(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), 
            "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, 
            res.Status().GetIssues().ToString());

        //TODO(st-shchetinin): Uncomment after supporting the entire export pipeline in FS
        // Fix: valid level
        {
            // NExport::TExportToFsSettings fixSettings = settings;
            // fixSettings.Compression("zstd-1");
            // auto res = YdbExportClient().ExportToFs(fixSettings).GetValueSync();
            // WaitOpSuccess(res, "Export with zstd-1 should succeed");
        }

        {
            // NExport::TExportToFsSettings fixSettings = settings;
            // fixSettings.Compression("zstd-22");
            // auto res = YdbExportClient().ExportToFs(fixSettings).GetValueSync();
            // WaitOpSuccess(res, "Export with zstd-22 should succeed");
        }
    }
}

// Import validation tests
Y_UNIT_TEST_SUITE_F(FsImportParamsValidationTest, TFsBackupParamsValidationTestFixture)
{
    Y_UNIT_TEST(NoBasePath) {
        // Test that base_path is required for import
        NImport::TImportFromFsSettings settings = MakeImportSettings("");

        auto res = YdbImportClient().ImportFromFs(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), 
            "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, 
            res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "base_path is required but not set",
            res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(RelativeBasePath) {
        // Test that base_path must be absolute for import
        NImport::TImportFromFsSettings settings = MakeImportSettings("");
        settings.BasePath("relative/path");
        settings.AppendItem({"table1", "/Root/Restored/table1"});  // Add item to pass items validation

        auto res = YdbImportClient().ImportFromFs(settings).GetValueSync();
        UNIT_ASSERT_C(!res.Status().IsSuccess(), 
            "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(res.Status().GetStatus(), NYdb::EStatus::BAD_REQUEST, 
            res.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(res.Status().GetIssues().ToString(), "base_path must be an absolute path",
            res.Status().GetIssues().ToString());

        //TODO(st-shchetinin): Uncomment after supporting the entire export pipeline in FS
        // Fix: use absolute path
        {
            // NImport::TImportFromFsSettings fixSettings = settings;
            // fixSettings.BasePath(TString(TempDir().Path()));
            // auto res = YdbImportClient().ImportFromFs(fixSettings).GetValueSync();
            // WaitOpSuccess(res, "Import with absolute base_path should succeed");
        }
    }
}


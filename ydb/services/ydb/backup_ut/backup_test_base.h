#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/services/ydb/ydb_common_ut.h>

#include <contrib/libs/fmt/include/fmt/format.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/is_in.h>
#include <util/generic/maybe.h>
#include <util/system/sanitizers.h>

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

class TBackupTestBaseFixture : public NUnitTest::TBaseFixture {
public:
    static constexpr TDuration DEFAULT_OPERATION_WAIT_TIME = NSan::PlainOrUnderSanitizer(TDuration::Seconds(60), TDuration::Seconds(300));
    static constexpr TDuration START_OPERATION_WAIT_TIME = NSan::PlainOrUnderSanitizer(TDuration::MilliSeconds(100), TDuration::Seconds(1));
    static constexpr TDuration MAX_OPERATION_WAIT_TIME = NSan::PlainOrUnderSanitizer(TDuration::Seconds(2), TDuration::Seconds(30));

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

protected:
    YDB_SDK_CLIENT(NYdb::NExport::TExportClient, YdbExportClient);
    YDB_SDK_CLIENT(NYdb::NImport::TImportClient, YdbImportClient);
    YDB_SDK_CLIENT(NYdb::NQuery::TQueryClient, YdbQueryClient);
    YDB_SDK_CLIENT(NYdb::NTable::TTableClient, YdbTableClient);
    YDB_SDK_CLIENT(NYdb::NScheme::TSchemeClient, YdbSchemeClient);
    YDB_SDK_CLIENT(NYdb::NOperation::TOperationClient, YdbOperationClient);

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

    size_t RestoreAttempt = 0;

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

    template <class TResponseType>
    TMaybe<NYdb::TOperation> WaitOpSuccess(const TResponseType& res, const TString& comments = {}, TDuration timeout = DEFAULT_OPERATION_WAIT_TIME) {
        return WaitOpStatus<TResponseType>(res, NYdb::EStatus::SUCCESS, comments, timeout);
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
    TMaybe<NYdb::TOperation> WaitOpStatus(const TResponseType& res, NYdb::EStatus status, const TString& comments = {}, TDuration timeout = DEFAULT_OPERATION_WAIT_TIME) {
        std::vector<NYdb::EStatus> statuses(1, status);
        return WaitOpStatus(res, statuses, comments, timeout);
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

private:
    void SetUp(NUnitTest::TTestContext& /* context */) override {
        using namespace fmt::literals;
        const bool isOlap = TStringBuf{Name_}.EndsWith("+IsOlap");

        auto res = YdbQueryClient().ExecuteQuery(fmt::format(R"sql(
            CREATE TABLE `/Root/RecursiveFolderProcessing/Table0` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );

            CREATE TABLE `/Root/RecursiveFolderProcessing/dir1/Table1` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );

            CREATE TABLE `/Root/RecursiveFolderProcessing/dir1/dir2/Table2` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );
        )sql", "store"_a = isOlap ? "COLUMN" : "ROW",
        "partition_count"_a = isOlap ? ", PARTITION_COUNT = 1" : ""), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        // Empty dir
        auto mkdir = YdbSchemeClient().MakeDirectory("/Root/RecursiveFolderProcessing/dir1/dir2/dir3").GetValueSync();
        UNIT_ASSERT_C(mkdir.IsSuccess(), mkdir.GetIssues().ToString());
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }
private:
    TDataShardExportFactory DataShardExportFactory;
    NKikimrConfig::TAppConfig AppConfig_;
    TMaybe<NYdb::TKikimrWithGrpcAndRootSchema> Server_;
    TMaybe<NYdb::TDriverConfig> DriverConfig;
    TMaybe<NYdb::TDriver> Driver;
};

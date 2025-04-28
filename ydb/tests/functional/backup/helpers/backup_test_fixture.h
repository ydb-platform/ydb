#pragma once
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/string/join.h>
#include <util/system/defaults.h>
#include <util/system/env.h>

class TBackupTestFixture : public NUnitTest::TBaseFixture {
public:
    TString YdbConnectionString() const {
        return TStringBuilder() << GetEnv("YDB_ENDPOINT") << "/?database=" << GetEnv("YDB_DATABASE");
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
        if (res.Ready()) {
            UNIT_ASSERT_C(res.Status().IsSuccess(), res.Status().GetIssues().ToString());
            return res;
        } else {
            TMaybe<NYdb::TOperation> op = res;
            WaitOp<TResponseType>(op);
            UNIT_ASSERT_C(op->Status().IsSuccess(), "Status: " << op->Status().GetStatus() << ". Issues: " << op->Status().GetIssues().ToString());
            return op;
        }
    }

    TString DebugListDir(const TString& path);

    TString S3Endpoint() const {
        return GetEnv("S3_ENDPOINT");
    }

    Aws::Client::ClientConfiguration& S3ClientConfig() {
        if (!AwsS3ClientConfig) {
            AwsS3ClientConfig.ConstructInPlace();
            AwsS3ClientConfig->endpointOverride = S3Endpoint();
            AwsS3ClientConfig->scheme = Aws::Http::Scheme::HTTP;
        }
        return *AwsS3ClientConfig;
    }

    Aws::S3::S3Client& S3Client() {
        if (!AwsS3Client) {
            AwsS3Client.ConstructInPlace(
                std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>(),
                S3ClientConfig(),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                false
            );
        }
        return *AwsS3Client;
    }

    void CreateBucket(const TString& bucketName) {
        NTestUtils::CreateBucket(bucketName, S3Client());
    }

    static void ValidateS3FileList(
        const TSet<TString>& paths,
        const TString& bucket, Aws::S3::S3Client& s3Client, const TString& prefix = {})
    {
        std::vector<TString> keysList = NTestUtils::GetObjectKeys(bucket, s3Client, prefix);
        TSet<TString> keys(keysList.begin(), keysList.end());
        UNIT_ASSERT_VALUES_EQUAL(keys, paths);
    }

    static void ValidateHasS3Files(
        const TSet<TString>& paths,
        const TString& bucket, Aws::S3::S3Client& s3Client, const TString& prefix = {})
    {
        std::vector<TString> keysList = NTestUtils::GetObjectKeys(bucket, s3Client, prefix);
        TSet<TString> keys(keysList.begin(), keysList.end());
        for (const auto& path : paths) {
            UNIT_ASSERT_C(keys.contains(path), "Path " << path << " is not found in S3. Existing paths: " << JoinSeq(", ", keysList));
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

protected:
    TMaybe<NYdb::TDriverConfig> DriverConfig;
    TMaybe<NYdb::TDriver> Driver;
    TMaybe<NYdb::NTable::TTableClient> TableClient;
    TMaybe<NYdb::NExport::TExportClient> ExportClient;
    TMaybe<NYdb::NImport::TImportClient> ImportClient;
    TMaybe<NYdb::NOperation::TOperationClient> OperationClient;
    TMaybe<NYdb::NQuery::TQueryClient> QueryClient;
    TMaybe<NYdb::NScheme::TSchemeClient> SchemeClient;
    TMaybe<Aws::Client::ClientConfiguration> AwsS3ClientConfig;
    TMaybe<Aws::S3::S3Client> AwsS3Client;
};

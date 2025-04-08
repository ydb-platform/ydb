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

    NYdb::NTable::TTableClient& YdbTableClient() {
        if (!TableClient) {
            TableClient.ConstructInPlace(YdbDriver());
        }
        return *TableClient;
    }

    NYdb::NExport::TExportClient& YdbExportClient() {
        if (!ExportClient) {
            ExportClient.ConstructInPlace(YdbDriver());
        }
        return *ExportClient;
    }

    NYdb::NImport::TImportClient& YdbImportClient() {
        if (!ImportClient) {
            ImportClient.ConstructInPlace(YdbDriver());
        }
        return *ImportClient;
    }

    NYdb::NQuery::TQueryClient& YdbQueryClient() {
        if (!QueryClient) {
            QueryClient.ConstructInPlace(YdbDriver());
        }
        return *QueryClient;
    }

    NYdb::NScheme::TSchemeClient& YdbSchemeClient() {
        if (!SchemeClient) {
            SchemeClient.ConstructInPlace(YdbDriver());
        }
        return *SchemeClient;
    }

    NYdb::NOperation::TOperationClient& YdbOperationClient() {
        if (!OperationClient) {
            OperationClient.ConstructInPlace(YdbDriver());
        }
        return *OperationClient;
    }

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

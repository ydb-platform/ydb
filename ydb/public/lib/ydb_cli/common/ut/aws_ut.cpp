#include <ydb/public/lib/ydb_cli/common/aws.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(AwsApiLifetime) {
    // Constructing an S3 client builds a CRT endpoint rule engine. Shutting the
    // SDK down while that client is still alive aborts in aws_mem_release.
    Y_UNIT_TEST(S3ClientIsReleasedBeforeSdkShutdown) {
#if !defined(_win32_)
        NYdb::NImport::TImportFromS3Settings settings;
        settings.Endpoint("https://localhost");
        settings.Bucket("bucket");
        settings.AccessKey("access");
        settings.SecretKey("secret");
        settings.UseVirtualAddressing(false);

        TAwsApiGuard awsApi;
        {
            const auto client = CreateS3ClientWrapper(settings);
            UNIT_ASSERT(client);
        }
        Y_UNUSED(awsApi);
#endif
    }
} // Y_UNIT_TEST_SUITE(AwsApiLifetime)

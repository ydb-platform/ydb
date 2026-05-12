#include "../yql_http_gateway.h"

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/generic/size_literals.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <ydb/core/util/aws.h>

#include <yql/essentials/utils/url_builder.h>

#include <cstdlib>

namespace {
    
TString Exec(const TString& cmd) {
    std::array<char, 128> buffer;
    TString result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

TString GetMinIoUrl() {
    auto dockerComposeBin = BinaryPath("library/recipes/docker_compose/bin/docker-compose");
    auto composeFileYml = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "docker-compose.yml");
    auto result = StringSplitter(Exec(dockerComposeBin + " -f " + composeFileYml + " port minio 9000")).Split(':').ToList<TString>();
    return result ? "http://localhost:" + Strip(result.back()) : TString{};
}

} // namespace

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    NKikimr::InitAwsAPI();
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    NKikimr::ShutdownAwsAPI();
}

Y_UNIT_TEST_SUITE(THttpGateway) {
    Y_UNIT_TEST(RetriesWithAwsCreds) {
        const auto httpGateway = NYql::IHTTPGateway::Make();
        const auto retryPolicy = NYql::IHTTPGateway::TRetryPolicy::GetFixedIntervalPolicy(
            [](CURLcode, long httpCode) {
                if (httpCode < 200 || httpCode >= 400) {
                    return ERetryErrorClass::LongRetry;
                }
                return ERetryErrorClass::NoRetry;
            },
            TDuration::Seconds(6),
            TDuration::Seconds(6),
            1,
            TDuration::Seconds(10)
        );

        const auto awsUserPwd = "minio:minio123";
        const auto awsSigV4 = "aws:amz:ru-central-1:s3";
        const auto requestUrl = GetMinIoUrl() + "/bucket/test.json";
        const auto requestHeaders = NYql::IHTTPGateway::MakeYcHeaders("0", "", {}, awsUserPwd, awsSigV4);

        auto promise = NThreading::NewPromise<std::optional<TString>>();
        
        const TString EXPECTED_BODY = "{\"key\": \"1\", \"value\": \"trololo\"}\n{\"key\": \"2\", \"value\": \"hello world\"}\n";

        // it takes at least three seconds for docker to create requested bucket,
        // so the first request is always gonna fail with code 404 and retried (which is intended)
        httpGateway->Download(
            requestUrl,
            requestHeaders,
            0,
            10_MB,
            [&promise, EXPECTED_BODY](NYql::IHTTPGateway::TResult&& result) {
                const long code = result.Content.HttpResponseCode;
                const TString body = result.Content.Extract();

                std::optional<TString> error;
                if (result.Issues) {
                    error = result.Issues.ToOneLineString();
                } else if (code < 200 || code >= 300) {
                    error = TStringBuilder() << "Invalid http return code, expected 206, got " << code;
                } else if (body != EXPECTED_BODY) {
                    error = TStringBuilder() << "Invalid value from minio, expected `" << EXPECTED_BODY << "`, got `" << body << "`";
                }

                promise.SetValue(error);
            },
            {},
            retryPolicy
        );
        
        auto future = promise.GetFuture();
        future.Wait();

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(future.ExtractValueSync(), std::nullopt);
    }
}

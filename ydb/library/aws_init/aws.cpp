#include "aws.h"
#include "cancellable_request.h"

#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/curl/CurlHttpClient.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/logging/ConsoleLogSystem.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/logging/DefaultLogSystem.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/curl/include/curl/curl.h>

#include <util/generic/scope.h>

#include <utility>

namespace {

using namespace Aws::Http;

class TCancellableCurlClient final : public CurlHttpClient {
    // The SDK calls its curl-options hook synchronously, after acquiring a handle.
    // Multiple SDK workers can use the same HTTP client concurrently.
    inline static thread_local HttpRequest* ActiveRequest = nullptr;

    static bool CanContinue(void* data) {
        const auto* request = static_cast<HttpRequest*>(data);
        return request->GetContinueRequestHandler()(request);
    }

    void OverrideOptionsOnConnectionHandle(CURL* handle) const override {
        if (ActiveRequest) {
            curl_easy_setopt(handle, CURLOPT_PREREQDATA, ActiveRequest);
            curl_easy_setopt(handle, CURLOPT_PREREQFUNCTION, +[](void* data, char*, char*, int, int) {
                return CanContinue(data) ? CURL_PREREQFUNC_OK : CURL_PREREQFUNC_ABORT;
            });
            curl_easy_setopt(handle, CURLOPT_XFERINFODATA, ActiveRequest);
            curl_easy_setopt(handle, CURLOPT_XFERINFOFUNCTION, +[](void* data, curl_off_t, curl_off_t, curl_off_t, curl_off_t) {
                return CanContinue(data) ? 0 : 1;
            });
            curl_easy_setopt(handle, CURLOPT_NOPROGRESS, 0L);
        }
    }

public:
    using CurlHttpClient::CurlHttpClient;

    std::shared_ptr<HttpResponse> MakeRequest(const std::shared_ptr<HttpRequest>& request,
            Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
            Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const override {
        const auto* check = request->GetContinueRequestHandler().target<NKikimr::TCancellableAwsRequest>();
        if (check && !(*check)(request.get())) {
            auto response = Aws::MakeShared<Standard::StandardHttpResponse>("YdbCurl", request);
            response->SetClientErrorType(Aws::Client::CoreErrors::USER_CANCELLED);
            response->SetClientErrorMessage("Request cancelled before dispatch");
            return response;
        }
        auto* previous = std::exchange(ActiveRequest, check ? request.get() : nullptr);
        Y_DEFER { ActiveRequest = previous; };
        return CurlHttpClient::MakeRequest(request, readLimiter, writeLimiter);
    }
};

class TYdbHttpClientFactory final : public HttpClientFactory {
    std::shared_ptr<HttpClient> CreateHttpClient(const Aws::Client::ClientConfiguration& config) const override {
        return Aws::MakeShared<TCancellableCurlClient>("YdbCurl", config);
    }

    std::shared_ptr<HttpRequest> CreateHttpRequest(const Aws::String& uri, HttpMethod method,
            const Aws::IOStreamFactory& streamFactory) const override {
        return CreateHttpRequest(URI(uri), method, streamFactory);
    }

    std::shared_ptr<HttpRequest> CreateHttpRequest(const URI& uri, HttpMethod method,
            const Aws::IOStreamFactory& streamFactory) const override {
        auto request = Aws::MakeShared<Standard::StandardHttpRequest>("YdbCurl", uri, method);
        request->SetResponseStreamFactory(streamFactory);
        return request;
    }
};

} // anonymous namespace

Aws::Utils::Logging::LogLevel ConvertLogLevel(int logLevel) {
    if (logLevel < 0) {
        return Aws::Utils::Logging::LogLevel::Off;
    } else if (logLevel > 6) {
        return Aws::Utils::Logging::LogLevel::Trace;
    } else {
        return static_cast<Aws::Utils::Logging::LogLevel>(logLevel);
    }
}

Aws::SDKOptions MakeDefaultOptions(const NKikimr::TAwsClientConfig& config) {
    static Aws::SDKOptions options;
    options.httpOptions.initAndCleanupCurl = false;
    options.httpOptions.httpClientFactory_create_fn = [] {
        return Aws::MakeShared<TYdbHttpClientFactory>("YdbCurl");
    };

    if (const auto logLevel = ConvertLogLevel(config.LogConfig.LogLevel); logLevel != Aws::Utils::Logging::LogLevel::Off) {
        const Aws::String filenamePrefix = config.LogConfig.FilenamePrefix.c_str();
        options.loggingOptions.logLevel = logLevel;
        options.loggingOptions.logger_create_fn = [logLevel, filenamePrefix] {
            std::shared_ptr<Aws::Utils::Logging::LogSystemInterface> result;
            if (filenamePrefix.empty()) {
                result = std::make_shared<Aws::Utils::Logging::ConsoleLogSystem>(logLevel);
            } else {
                result = std::make_shared<Aws::Utils::Logging::DefaultLogSystem>(logLevel, filenamePrefix);
            }
            return result;
        };
    }

    return options;
}

namespace NKikimr {

void InitAwsAPI(const TAwsClientConfig& config) {
    curl_global_init(CURL_GLOBAL_ALL);
    Aws::InitAPI(MakeDefaultOptions(config));
    Aws::Internal::CleanupEC2MetadataClient(); // speeds up config construction
}

void ShutdownAwsAPI(const TAwsClientConfig& config) {
    Aws::ShutdownAPI(MakeDefaultOptions(config));
    curl_global_cleanup();
}

} // NKikimr

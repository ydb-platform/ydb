#include "aws.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/logging/ConsoleLogSystem.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/logging/DefaultLogSystem.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/curl/include/curl/curl.h>

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

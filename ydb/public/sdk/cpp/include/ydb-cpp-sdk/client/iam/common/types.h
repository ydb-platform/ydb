#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/jwt/jwt.h>

#include <util/datetime/base.h>

#include <fstream>
#include <string>

namespace NYdb {
namespace NIam {

constexpr std::string_view DEFAULT_ENDPOINT = "iam.api.cloud.yandex.net";
constexpr bool DEFAULT_ENABLE_SSL = true;

constexpr std::string_view DEFAULT_HOST = "169.254.169.254";
constexpr uint32_t DEFAULT_PORT = 80;

constexpr TDuration DEFAULT_REFRESH_PERIOD = TDuration::Hours(1);
constexpr TDuration DEFAULT_REQUEST_TIMEOUT = TDuration::Seconds(10);

}

struct TIamHost {
    std::string Host = std::string(NIam::DEFAULT_HOST);
    uint32_t Port = NIam::DEFAULT_PORT;
    TDuration RefreshPeriod = NIam::DEFAULT_REFRESH_PERIOD;
};

struct TIamEndpoint {
    std::string Endpoint = std::string(NIam::DEFAULT_ENDPOINT);
    TDuration RefreshPeriod = NIam::DEFAULT_REFRESH_PERIOD;
    TDuration RequestTimeout = NIam::DEFAULT_REQUEST_TIMEOUT;
    bool EnableSsl = NIam::DEFAULT_ENABLE_SSL;
};

struct TIamJwtFilename : TIamEndpoint { std::string JwtFilename; };

struct TIamJwtContent : TIamEndpoint { std::string JwtContent; };

struct TIamJwtParams : TIamEndpoint { TJwtParams JwtParams; };

struct TIamOAuth : TIamEndpoint { std::string OAuthToken; };


inline TJwtParams ReadJwtKeyFile(const std::string& filename) {
    std::ifstream input(filename, std::ios::in);
    return ParseJwtParams({std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()});
}

}

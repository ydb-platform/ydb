#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <ydb/public/lib/jwt/jwt.h>
#include <util/datetime/base.h>

namespace NYdb {

namespace NIam {
constexpr TStringBuf DEFAULT_ENDPOINT = "iam.api.cloud.yandex.net";

constexpr TStringBuf DEFAULT_HOST = "169.254.169.254";
constexpr ui32 DEFAULT_PORT = 80;

constexpr TDuration DEFAULT_REFRESH_PERIOD = TDuration::Hours(1);
constexpr TDuration DEFAULT_REQUEST_TIMEOUT = TDuration::Seconds(10);
}

struct TIamHost {
    TString Host = TString(NIam::DEFAULT_HOST);
    ui32 Port = NIam::DEFAULT_PORT;
    TDuration RefreshPeriod = NIam::DEFAULT_REFRESH_PERIOD;
};

struct TIamEndpoint {
    TString Endpoint = TString(NIam::DEFAULT_ENDPOINT);
    TDuration RefreshPeriod = NIam::DEFAULT_REFRESH_PERIOD;
    TDuration RequestTimeout = NIam::DEFAULT_REQUEST_TIMEOUT;
};

struct TIamJwtFilename : TIamEndpoint { TString JwtFilename; };

struct TIamJwtContent : TIamEndpoint { TString JwtContent; };

struct TIamOAuth : TIamEndpoint { TString OAuthToken; };

/// Acquire an IAM token using a local metadata service on a virtual machine.
TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(const TIamHost& params = {});

/// Acquire an IAM token using a JSON Web Token (JWT) file name.
TCredentialsProviderFactoryPtr CreateIamJwtFileCredentialsProviderFactory(const TIamJwtFilename& params);

/// Acquire an IAM token using JSON Web Token (JWT) contents.
TCredentialsProviderFactoryPtr CreateIamJwtParamsCredentialsProviderFactory(const TIamJwtContent& param);

// Acquire an IAM token using a user OAuth token.
TCredentialsProviderFactoryPtr CreateIamOAuthCredentialsProviderFactory(const TIamOAuth& params);

} // namespace NYdb

#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <util/generic/string.h>

#include <memory>

namespace NKikimr {

struct TYdbCredentialsSettings {
    bool UseLocalMetadata = false;
    TString OAuthToken;

    TString SaKeyFile;
    TString IamEndpoint;
};

using TYdbCredentialsProviderFactory = std::function<std::shared_ptr<NYdb::ICredentialsProviderFactory>(const TYdbCredentialsSettings& settings)>;

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateYdbCredentialsProviderFactory(const TYdbCredentialsSettings& settings);

}

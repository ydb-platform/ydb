#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <util/generic/string.h>
#include <util/stream/fwd.h>

namespace YAML {
class Node;
}

namespace NYdb::NConsoleClient {

class TAuthMethodOption;
class TClientCommandOptions;
class TOptionsParseResult;

struct TOidcCliOptions {
    TString ConfigFile;
    TString Issuer;
    TString Flow;
    TString ClientId;
    TString ClientSecret;
    TString AccessToken;
    TString ExpiresAt;
    TString Scope;
    TString CachePath;

    bool IsConfigured() const;
    bool HasOptions() const;
    NOidc::TOidcConfig MakeConfig() const;
    YAML::Node MakeProfileAuth() const;
    void Print(IOutputStream& output) const;
};

struct TOidcAuthOptions {
    TAuthMethodOption& Config;
    TAuthMethodOption& Issuer;
};

TOidcAuthOptions AddOidcOptions(TClientCommandOptions& options, TOidcCliOptions& values, bool profileCommand);
void ResolveOidcOptions(TOidcCliOptions& values, const TOptionsParseResult& result);

} // namespace NYdb::NConsoleClient

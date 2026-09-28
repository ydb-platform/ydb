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
    TString Issuer;
    TString Flow;
    TString ClientId;
    TString ClientSecret;
    TString ClientSecretFile;
    TString AccessTokenFile;
    TString Scope;
    TString CachePath;

    bool IsConfigured() const;
    bool HasOptions() const;
    NOidc::TOidcConfig MakeConfig() const;
    YAML::Node MakeProfileAuth() const;
    void Print(IOutputStream& output) const;
};

TAuthMethodOption& AddOidcOptions(TClientCommandOptions& options, TOidcCliOptions& values, bool profileCommand);
void ResolveOidcOptions(TOidcCliOptions& values, const TOptionsParseResult& result);

} // namespace NYdb::NConsoleClient

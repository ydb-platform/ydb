#include "settings.h"

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/parser.h>

namespace NYdb {

TCommonClientSettings& TCommonClientSettings::AuthToken(const TMaybe<TStringType>& token) {
    return CredentialsProviderFactory(CreateOAuthCredentialsProviderFactory(token.GetRef()));
}

TCommonClientSettings GetClientSettingsFromConnectionString(const TStringType& connectionString) {
    TCommonClientSettings settings;
    auto connectionInfo = ParseConnectionString(connectionString);
    settings.Database(connectionInfo.Database);
    settings.DiscoveryEndpoint(connectionInfo.Endpoint);
    settings.SslCredentials(TSslCredentials(connectionInfo.EnableSsl));
    return settings;
}

} // namespace NYdb

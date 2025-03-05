#include <ydb-cpp-sdk/client/common_client/settings.h>

#include <src/client/impl/ydb_internal/common/parser.h>

namespace NYdb::inline V3 {

TCommonClientSettings& TCommonClientSettings::AuthToken(const std::optional<std::string>& token) {
    return CredentialsProviderFactory(CreateOAuthCredentialsProviderFactory(token.value()));
}

TCommonClientSettings GetClientSettingsFromConnectionString(const std::string& connectionString) {
    TCommonClientSettings settings;
    auto connectionInfo = ParseConnectionString(connectionString);
    settings.Database(connectionInfo.Database);
    settings.DiscoveryEndpoint(connectionInfo.Endpoint);
    settings.SslCredentials(TSslCredentials(connectionInfo.EnableSsl));
    return settings;
}

} // namespace NYdb

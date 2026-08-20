#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/getopt/last_getopt.h>

#include <iostream>

namespace {

    class TConsoleAcceptor final: public NYdb::IOidcAuthorizationAcceptor {
    public:
        void Accept(const NYdb::TOidcDeviceAuthorizationInfo& info) override {
            std::cout << "Open " << info.VerificationUri << " and enter code " << info.UserCode << std::endl;
        }
    };

} // namespace

int main(int argc, char** argv) {
    std::string endpoint;
    std::string database;
    std::string issuer;
    std::string flow;
    std::string clientId;
    std::string clientSecret;
    std::string accessToken;
    std::string refreshToken;
    std::vector<std::string> scopes;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT").StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database").Required().RequiredArgument("PATH").StoreResult(&database);
    opts.AddLongOption("issuer", "OIDC issuer URL").Required().RequiredArgument("URL").StoreResult(&issuer);
    opts.AddLongOption("flow", "Authentication flow: static, client, or device").Required().RequiredArgument("FLOW").StoreResult(&flow);
    opts.AddLongOption("client-id", "OIDC client id").RequiredArgument("ID").StoreResult(&clientId);
    opts.AddLongOption("client-secret", "OIDC client secret for Client Credentials Grant").RequiredArgument("SECRET").StoreResult(&clientSecret);
    opts.AddLongOption("access-token", "Static access token").RequiredArgument("TOKEN").StoreResult(&accessToken);
    opts.AddLongOption("refresh-token", "Optional static refresh token").RequiredArgument("TOKEN").StoreResult(&refreshToken);
    opts.AddLongOption("scope", "Additional OAuth scope").RequiredArgument("SCOPE").AppendTo(&scopes);
    NLastGetopt::TOptsParseResult optsResult(&opts, argc, argv);

    NYdb::TOidcConfig oidc;
    oidc.Issuer(issuer);
    if (flow == "static") {
        if (accessToken.empty()) {
            throw std::invalid_argument("Static flow requires --access-token");
        }
        NYdb::TOidcTokenSet tokens{.AccessToken = NYdb::TOidcToken{.Token = accessToken}};
        if (!refreshToken.empty()) {
            tokens.RefreshToken = NYdb::TOidcToken{.Token = refreshToken};
        }
        oidc.Flow(NYdb::TStaticOidcConfig().ClientId(clientId).Tokens(tokens));
    } else if (flow == "client") {
        if (clientId.empty() || clientSecret.empty()) {
            throw std::invalid_argument("Client flow requires --client-id and --client-secret");
        }
        NYdb::TClientOidcConfig config;
        config.ClientId(clientId).ClientSecret(clientSecret);
        for (const auto& scope : scopes) {
            config.AppendScope(scope);
        }
        oidc.Flow(config);
    } else if (flow == "device") {
        if (clientId.empty()) {
            throw std::invalid_argument("Device flow requires --client-id");
        }
        NYdb::TDeviceOidcConfig config;
        config.ClientId(clientId);
        for (const auto& scope : scopes) {
            config.AppendScope(scope);
        }
        oidc.Flow(config).Acceptor(std::make_shared<TConsoleAcceptor>());
    } else {
        throw std::invalid_argument("Unknown OIDC flow");
    }

    NYdb::TDriver driver(
        NYdb::TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDatabase(database)
            .SetCredentialsProviderFactory(NYdb::CreateOidcCredentialsProviderFactory(oidc)));
    driver.Stop(true);
    return 0;
}

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
    std::string oidcConfigPath;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT").StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database").Required().RequiredArgument("PATH").StoreResult(&database);
    opts.AddLongOption("oidc-config", "OIDC configuration YAML file").Required().RequiredArgument("PATH").StoreResult(&oidcConfigPath);
    NLastGetopt::TOptsParseResult optsResult(&opts, argc, argv);

    NYdb::TOidcConfig oidc = NYdb::TOidcConfig::Parse(oidcConfigPath);
    if (std::holds_alternative<NYdb::TDeviceOidcConfig>(oidc.Flow_)) {
        oidc.Acceptor(std::make_shared<TConsoleAcceptor>());
    }

    NYdb::TDriver driver(
        NYdb::TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDatabase(database)
            .SetCredentialsProviderFactory(NYdb::CreateOidcCredentialsProviderFactory(oidc)));
    driver.Stop(true);
    return 0;
}

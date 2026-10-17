#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <util/system/mutex.h>

#include <iostream>

class TConsoleAcceptor final: public NYdb::NOidc::IAuthAcceptor {
public:
    void Accept(const NYdb::NOidc::TDeviceAuthInfo& info) override;

private:
    static void Print(const std::string& value);
};

class TMemoryTokenCacher final: public NYdb::NOidc::ITokenCacher {
public:
    std::optional<NYdb::NOidc::TTokenCache> Read() const override;
    void Write(const NYdb::NOidc::TTokenCache& tokens) override;

private:
    mutable TMutex Mutex;
    std::optional<NYdb::NOidc::TTokenCache> Tokens;
};

void TConsoleAcceptor::Accept(const NYdb::NOidc::TDeviceAuthInfo& info) {
    static TMutex outputMutex;
    with_lock (outputMutex) {
        std::cerr << "Open ";
        Print(info.VerificationUrl);
        std::cerr << " and enter code ";
        Print(info.UserCode);
        std::cerr << std::endl;
        if (info.VerificationUrlComplete.has_value()) {
            std::cerr << "Or open ";
            Print(*info.VerificationUrlComplete);
            std::cerr << std::endl;
        }
    }
}

void TConsoleAcceptor::Print(const std::string& value) {
    for (const unsigned char ch : value) {
        std::cerr << (ch >= 0x20 && ch < 0x7f ? static_cast<char>(ch) : '?');
    }
}

std::optional<NYdb::NOidc::TTokenCache> TMemoryTokenCacher::Read() const {
    with_lock (Mutex) {
        return Tokens;
    }
}

void TMemoryTokenCacher::Write(const NYdb::NOidc::TTokenCache& tokens) {
    with_lock (Mutex) {
        Tokens = tokens;
    }
}

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "Usage: oidc <grpc[s]://endpoint:port> <database> <issuer> <client-id>" << std::endl;
        return 1;
    }
    try {
        NYdb::NOidc::TOidcConfig oidcConfig{
            .Issuer = argv[3],
            .FlowConfig = NYdb::NOidc::TDeviceOidcConfig{
                .ClientId = argv[4],
                .Scopes = {"openid", "user-context"},
            },
        };
        oidcConfig
            .Cacher(std::make_shared<TMemoryTokenCacher>())
            .Acceptor(std::make_shared<TConsoleAcceptor>());

        auto config = NYdb::TDriverConfig(argv[1])
            .SetDatabase(argv[2])
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
            .SetCredentialsProviderFactory(NYdb::NOidc::CreateOidcProviderFactory(oidcConfig));
        NYdb::TDriver driver(config);
        NYdb::NQuery::TQueryClient client(driver);
        auto result = client.ExecuteQuery("SELECT 1", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        if (!result.IsSuccess()) {
            std::cerr << result.GetIssues().ToString() << std::endl;
            return 1;
        }
        std::cout << "Query succeeded" << std::endl;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}

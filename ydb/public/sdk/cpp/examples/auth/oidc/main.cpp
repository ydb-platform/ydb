#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/from_file.h>

#include <iostream>

class TConsoleAcceptor final: public NYdb::NOidc::IAuthAcceptor {
public:
    void Accept(const NYdb::NOidc::TDeviceAuthInfo& info) override;

private:
    static void Print(const std::string& value);
};

void TConsoleAcceptor::Accept(const NYdb::NOidc::TDeviceAuthInfo& info) {
    std::cerr << "Open ";
    Print(info.VerificationUrl);
    std::cerr << " and enter code ";
    Print(info.UserCode);
    std::cerr << std::endl;
}

void TConsoleAcceptor::Print(const std::string& value) {
    for (const unsigned char ch : value) {
        std::cerr << (ch >= 0x20 && ch < 0x7f ? static_cast<char>(ch) : '?');
    }
}

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "Usage: oidc <grpcs://endpoint:port> <database> <oidc.yaml>" << std::endl;
        return 1;
    }
    try {
        auto config = NYdb::TDriverConfig()
            .SetEndpoint(argv[1])
            .SetDatabase(argv[2])
            .SetCredentialsProviderFactory(NYdb::NOidc::CreateOidcFileCredentialsProviderFactory(
                argv[3], std::make_shared<TConsoleAcceptor>()));
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

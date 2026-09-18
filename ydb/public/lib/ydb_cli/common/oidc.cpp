#include "oidc.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/from_file.h>

#include <util/stream/output.h>

#include <mutex>

namespace NYdb::NConsoleClient {
namespace {

std::string DisplayText(const std::string& text) {
    static constexpr char Hex[] = "0123456789ABCDEF";
    std::string result;
    for (const unsigned char c : text) {
        if (c >= 0x20 && c < 0x7f) {
            result += static_cast<char>(c);
        } else {
            result += "\\x";
            result += Hex[c >> 4];
            result += Hex[c & 15];
        }
    }
    return result;
}

class TCliAuthAcceptor final: public IAuthAcceptor {
public:
    void Accept(const TDeviceAuthInfo& info) override {
        static std::mutex outputMutex;
        std::lock_guard lock(outputMutex);
        Cerr << "To sign in, open " << DisplayText(info.VerificationUrl)
             << " and enter code " << DisplayText(info.UserCode) << Endl;
        if (info.VerificationUrlComplete) {
            Cerr << "Or open " << DisplayText(*info.VerificationUrlComplete) << Endl;
        }
    }
};

} // namespace

std::shared_ptr<ICredentialsProviderFactory> CreateCliOidcCredentialsProviderFactory(const TString& configPath) {
    return CreateOidcFileCredentialsProviderFactory(std::string(configPath), std::make_shared<TCliAuthAcceptor>());
}

} // namespace NYdb::NConsoleClient

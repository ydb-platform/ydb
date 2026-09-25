#include "oidc.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/from_file.h>

#include <util/stream/output.h>

#include <util/system/mutex.h>

namespace NYdb::NConsoleClient {
namespace {

std::string DisplayText(const std::string& text);

class TCliAuthAcceptor final: public NOidc::IAuthAcceptor {
public:
    void Accept(const NOidc::TDeviceAuthInfo& info) override;
};

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

void TCliAuthAcceptor::Accept(const NOidc::TDeviceAuthInfo& info) {
    static TMutex outputMutex;
    with_lock (outputMutex) {
        Cerr << "To sign in, open " << DisplayText(info.VerificationUrl)
             << " and enter code " << DisplayText(info.UserCode) << Endl;
        if (info.VerificationUrlComplete.has_value()) {
            Cerr << "Or open " << DisplayText(*info.VerificationUrlComplete) << Endl;
        }
    }
}

} // namespace

std::shared_ptr<ICredentialsProviderFactory> CreateCliOidcCredentialsProviderFactory(const TString& configPath) {
    return NOidc::CreateOidcFileCredentialsProviderFactory(std::string(configPath), std::make_shared<TCliAuthAcceptor>());
}

} // namespace NYdb::NConsoleClient

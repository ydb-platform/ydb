#include "oidc.h"
#include "oidc_config.h"
#include "oidc_options.h"

#include <util/stream/output.h>
#include <util/system/mutex.h>

namespace NYdb::NConsoleClient {
namespace {

std::string DisplayText(const std::string& text);

class TCliAuthAcceptor final: public NOidc::IAuthAcceptor {
public:
    explicit TCliAuthAcceptor(IOutputStream& output);

    void Accept(const NOidc::TDeviceAuthInfo& info) override;

private:
    IOutputStream& Output;
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

TCliAuthAcceptor::TCliAuthAcceptor(IOutputStream& output)
    : Output(output)
{
}

void TCliAuthAcceptor::Accept(const NOidc::TDeviceAuthInfo& info) {
    static TMutex outputMutex;
    with_lock (outputMutex) {
        Output << "To sign in, open " << DisplayText(info.VerificationUrl)
               << " and enter code " << DisplayText(info.UserCode) << Endl;
        if (info.VerificationUrlComplete.has_value()) {
            Output << "Or open " << DisplayText(*info.VerificationUrlComplete) << Endl;
        }
    }
}

} // namespace

std::shared_ptr<NOidc::IAuthAcceptor> CreateCliAuthAcceptor(IOutputStream& output) {
    return std::make_shared<TCliAuthAcceptor>(output);
}

std::shared_ptr<ICredentialsProviderFactory> CreateCliOidcCredentialsProviderFactory(const TString& configPath) {
    return CreateOidcFileCredentialsProviderFactory(std::string(configPath), CreateCliAuthAcceptor(Cerr));
}

std::shared_ptr<ICredentialsProviderFactory> CreateCliOidcCredentialsProviderFactory(const TOidcCliOptions& options) {
    auto config = options.MakeConfig();
    config.Acceptor(CreateCliAuthAcceptor(Cerr));
    return NOidc::CreateOidcProviderFactory(config);
}

} // namespace NYdb::NConsoleClient

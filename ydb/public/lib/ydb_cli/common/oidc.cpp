#include "oidc.h"

#include <ydb/public/lib/ydb_cli/common/oidc_config.h>
#include <ydb/public/lib/ydb_cli/common/oidc_options.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <util/stream/output.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <memory>
#include <string>
#include <utility>

namespace NYdb::NConsoleClient {
namespace {

std::string DisplayText(const std::string& text);

class TCliOidcCredentialsProviderFactory final: public ICredentialsProviderFactory {
public:
    explicit TCliOidcCredentialsProviderFactory(TCredentialsProviderFactoryPtr factory);

    TCredentialsProviderPtr CreateProvider() const override;
    std::string GetClientIdentity() const override;

private:
    const TCredentialsProviderFactoryPtr Factory;
    mutable TMutex Mutex;
    mutable TCredentialsProviderPtr Provider;
};

class TCliAuthAcceptor final: public NOidc::IAuthAcceptor {
public:
    explicit TCliAuthAcceptor(IOutputStream& output);

    void Accept(const NOidc::TDeviceAuthInfo& info) override;

private:
    IOutputStream& Output;
};

TCliOidcCredentialsProviderFactory::TCliOidcCredentialsProviderFactory(TCredentialsProviderFactoryPtr factory)
    : Factory(std::move(factory))
{
}

TCredentialsProviderPtr TCliOidcCredentialsProviderFactory::CreateProvider() const {
    with_lock (Mutex) {
        if (Provider == nullptr) {
            Provider = Factory->CreateProvider();
        }
        return Provider;
    }
}

std::string TCliOidcCredentialsProviderFactory::GetClientIdentity() const {
    return Factory->GetClientIdentity();
}

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
    return std::make_shared<TCliOidcCredentialsProviderFactory>(
        CreateOidcFileCredentialsProviderFactory(std::string(configPath), CreateCliAuthAcceptor(Cerr)));
}

std::shared_ptr<ICredentialsProviderFactory> CreateCliOidcCredentialsProviderFactory(const TOidcCliOptions& options) {
    auto config = options.ResolvedConfig.has_value() ? options.ResolvedConfig.value() : options.MakeConfig();
    config.Acceptor(CreateCliAuthAcceptor(Cerr));
    return std::make_shared<TCliOidcCredentialsProviderFactory>(NOidc::CreateOidcProviderFactory(config));
}

} // namespace NYdb::NConsoleClient

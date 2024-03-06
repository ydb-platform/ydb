#include "server.h"
#include "config.h"

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/crypto/tls.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/concurrency/poller.h>

namespace NYT::NHttps {

using namespace NNet;
using namespace NHttp;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    explicit TServer(IServerPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    void AddHandler(
        const TString& pattern,
        const IHttpHandlerPtr& handler) override
    {
        Underlying_->AddHandler(pattern, handler);
    }

    const TNetworkAddress& GetAddress() const override
    {
        return Underlying_->GetAddress();
    }

    //! Starts the server.
    void Start() override
    {
        Underlying_->Start();
    }

    //! Stops the server.
    void Stop() override
    {
        Underlying_->Stop();
    }

    void SetPathMatcher(const IRequestPathMatcherPtr& matcher) override
    {
        Underlying_->SetPathMatcher(matcher);
    }

    IRequestPathMatcherPtr GetPathMatcher() override
    {
        return Underlying_->GetPathMatcher();
    }

private:
    const IServerPtr Underlying_;
};

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IPollerPtr& poller,
    const IPollerPtr& acceptor)
{
    auto sslContext =  New<TSslContext>();
    if (config->Credentials->CertChain->FileName) {
        sslContext->AddCertificateChainFromFile(*config->Credentials->CertChain->FileName);
    } else if (config->Credentials->CertChain->Value) {
        sslContext->AddCertificateChain(*config->Credentials->CertChain->Value);
    } else {
        YT_ABORT();
    }
    if (config->Credentials->PrivateKey->FileName) {
        sslContext->AddPrivateKeyFromFile(*config->Credentials->PrivateKey->FileName);
    } else if (config->Credentials->PrivateKey->Value) {
        sslContext->AddPrivateKey(*config->Credentials->PrivateKey->Value);
    } else {
        YT_ABORT();
    }

    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    auto tlsListener = sslContext->CreateListener(address, poller, acceptor);

    auto configCopy = CloneYsonStruct(config);
    configCopy->IsHttps = true;
    auto httpServer = NHttp::CreateServer(configCopy, tlsListener, poller, acceptor);

    return New<TServer>(std::move(httpServer));
}

IServerPtr CreateServer(const TServerConfigPtr& config, const IPollerPtr& poller)
{
    return CreateServer(config, poller, poller);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps

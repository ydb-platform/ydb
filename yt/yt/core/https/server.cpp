#include "server.h"
#include "config.h"

#include <yt/yt/core/http/server.h>
#include <yt/yt/core/http/private.h>

#include <yt/yt/core/crypto/tls.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NHttps {

static const auto& Logger = NHttp::HttpLogger;

using namespace NNet;
using namespace NHttp;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    explicit TServer(IServerPtr underlying, TPeriodicExecutorPtr certificateUpdater)
        : Underlying_(std::move(underlying))
        , CertificateUpdater_(certificateUpdater)
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
        if (CertificateUpdater_) {
            CertificateUpdater_->Start();
        }
    }

    //! Stops the server.
    void Stop() override
    {
        Underlying_->Stop();
        if (CertificateUpdater_) {
            YT_UNUSED_FUTURE(CertificateUpdater_->Stop());
        }
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
    const TPeriodicExecutorPtr CertificateUpdater_;
};

static void ApplySslConfig(const TSslContextPtr&  sslContext, const TServerCredentialsConfigPtr& sslConfig)
{
    if (sslConfig->CertChain->FileName) {
        sslContext->AddCertificateChainFromFile(*sslConfig->CertChain->FileName);
    } else if (sslConfig->CertChain->Value) {
        sslContext->AddCertificateChain(*sslConfig->CertChain->Value);
    } else {
        YT_ABORT();
    }
    if (sslConfig->PrivateKey->FileName) {
        sslContext->AddPrivateKeyFromFile(*sslConfig->PrivateKey->FileName);
    } else if (sslConfig->PrivateKey->Value) {
        sslContext->AddPrivateKey(*sslConfig->PrivateKey->Value);
    } else {
        YT_ABORT();
    }
}

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IPollerPtr& poller,
    const IPollerPtr& acceptor)
{
    auto sslContext =  New<TSslContext>();
    ApplySslConfig(sslContext, config->Credentials);
    sslContext->Commit();

    auto sslConfig = config->Credentials;
    TPeriodicExecutorPtr certificateUpdater;
    if (sslConfig->UpdatePeriod &&
        sslConfig->CertChain->FileName &&
        sslConfig->PrivateKey->FileName)
    {
        certificateUpdater = New<TPeriodicExecutor>(
            poller->GetInvoker(),
            BIND([=, serverName = config->ServerName] {
                try {
                    auto modificationTime = Max(
                        NFS::GetPathStatistics(*sslConfig->CertChain->FileName).ModificationTime,
                        NFS::GetPathStatistics(*sslConfig->PrivateKey->FileName).ModificationTime);

                    // Detect fresh and stable updates.
                    if (modificationTime > sslContext->GetCommitTime() &&
                        modificationTime + sslConfig->UpdatePeriod <= TInstant::Now())
                    {
                        YT_LOG_INFO("Updating TLS certificates (ServerName: %v, ModificationTime: %v)", serverName, modificationTime);
                        sslContext->Reset();
                        ApplySslConfig(sslContext, sslConfig);
                        sslContext->Commit(modificationTime);
                        YT_LOG_INFO("TLS certificates updated (ServerName: %v)", serverName);
                    }
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Unexpected exception while updating TLS certificates (ServerName: %v)", serverName);
                }
            }),
            sslConfig->UpdatePeriod);
    }

    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    auto tlsListener = sslContext->CreateListener(address, poller, acceptor);

    auto configCopy = CloneYsonStruct(config);
    configCopy->IsHttps = true;
    auto httpServer = NHttp::CreateServer(configCopy, tlsListener, poller, acceptor);

    return New<TServer>(std::move(httpServer), std::move(certificateUpdater));
}

IServerPtr CreateServer(const TServerConfigPtr& config, const IPollerPtr& poller)
{
    return CreateServer(config, poller, poller);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps

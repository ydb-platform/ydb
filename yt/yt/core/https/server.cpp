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
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHttps {

constinit const auto Logger = NHttp::HttpLogger;

using namespace NNet;
using namespace NHttp;
using namespace NCrypto;
using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    TServer(IServerPtr underlying, TPeriodicExecutorPtr certificateUpdater, TPeriodicExecutorPtr certificateSensorsUpdater)
        : Underlying_(std::move(underlying))
        , CertificateUpdater_(certificateUpdater)
        , CertificateSensorsUpdater_(std::move(certificateSensorsUpdater))
    { }

    void AddHandler(
        const std::string& pattern,
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
        if (CertificateSensorsUpdater_) {
            CertificateSensorsUpdater_->Start();
        }
    }

    //! Stops the server.
    void Stop() override
    {
        Underlying_->Stop();
        if (CertificateUpdater_) {
            YT_UNUSED_FUTURE(CertificateUpdater_->Stop());
        }
        if (CertificateSensorsUpdater_) {
            YT_UNUSED_FUTURE(CertificateSensorsUpdater_->Stop());
        }
        if (OwnPoller_) {
            OwnPoller_->Shutdown();
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

    void SetOwnPoller(IPollerPtr poller)
    {
        OwnPoller_ = std::move(poller);
    }

private:
    const IServerPtr Underlying_;
    const TPeriodicExecutorPtr CertificateUpdater_;
    const TPeriodicExecutorPtr CertificateSensorsUpdater_;
    IPollerPtr OwnPoller_;
};

static void ApplySslConfig(const TSslContextPtr& sslContext, const TServerCredentialsConfigPtr& sslConfig)
{
    sslContext->ApplyConfig(sslConfig);
}

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IPollerPtr& poller,
    const IPollerPtr& acceptor,
    const IInvokerPtr& controlInvoker,
    std::optional<NCrypto::TCertProfiler> certProfiler)
{
    auto sslContext = New<TSslContext>();
    ApplySslConfig(sslContext, config->Credentials);
    sslContext->Commit();

    auto sslConfig = config->Credentials;
    TPeriodicExecutorPtr certificateUpdater;
    if (sslConfig->UpdatePeriod &&
        sslConfig->CertificateChain->FileName &&
        sslConfig->PrivateKey->FileName)
    {
        YT_VERIFY(controlInvoker);
        certificateUpdater = New<TPeriodicExecutor>(
            controlInvoker,
            BIND([=] {
                try {
                    auto modificationTime = Max(
                        NFS::GetPathStatistics(*sslConfig->CertificateChain->FileName).ModificationTime,
                        NFS::GetPathStatistics(*sslConfig->PrivateKey->FileName).ModificationTime);

                    // Detect fresh and stable updates.
                    if (modificationTime > sslContext->GetCommitTime() &&
                        modificationTime + sslConfig->UpdatePeriod <= TInstant::Now())
                    {
                        YT_LOG_INFO("Updating TLS certificates (ServerName: %v, ModificationTime: %v)",
                            config->ServerName,
                            modificationTime);
                        sslContext->Reset();
                        ApplySslConfig(sslContext, sslConfig);
                        sslContext->Commit(modificationTime);
                        YT_LOG_INFO("TLS certificates updated (ServerName: %v)",
                            config->ServerName);
                    }
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex,
                        "Unexpected exception while updating TLS certificates (ServerName: %v)",
                        config->ServerName);
                }
            }),
            sslConfig->UpdatePeriod);
    }

    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    auto tlsListener = sslContext->CreateListener(address, poller, acceptor);

    auto configCopy = CloneYsonStruct(config);
    configCopy->IsHttps = true;
    auto httpServer = NHttp::CreateServer(
        configCopy,
        tlsListener,
        poller,
        acceptor);

    TPeriodicExecutorPtr certificateSensorsUpdater;
    if (certProfiler && sslConfig->CertificateChain) {
        auto certChainToExpiry = certProfiler->Profiler.Gauge("/cert_chain_to_expiry");
        // Update expiry time ASAP after creation.
        certChainToExpiry.Update(GetCertTimeToExpiry(sslConfig->CertificateChain));

        certificateSensorsUpdater = New<TPeriodicExecutor>(
            certProfiler->Invoker,
            BIND([sslConfig, certChainToExpiry] {
                try {
                    certChainToExpiry.Update(GetCertTimeToExpiry(sslConfig->CertificateChain));
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Failed to update HTTPS server certificate sensors");
                }
            }),
            sslConfig->UpdatePeriod);
    }

    return New<TServer>(
        std::move(httpServer),
        std::move(certificateUpdater),
        std::move(certificateSensorsUpdater));
}

IServerPtr CreateServer(const TServerConfigPtr& config, const IPollerPtr& poller)
{
    return CreateServer(config, poller, poller, /*controlInvoker*/ nullptr);
}

IServerPtr CreateServer(const TServerConfigPtr& config, int pollerThreadCount)
{
    auto poller = CreateThreadPoolPoller(pollerThreadCount, config->ServerName);
    auto server = CreateServer(config, poller);
    StaticPointerCast<TServer>(server)->SetOwnPoller(std::move(poller));
    return server;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps

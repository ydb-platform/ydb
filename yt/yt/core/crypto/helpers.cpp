#include "helpers.h"

#include "config.h"
#include "tls.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NCrypto {

using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsSslCertificateUpdateEnabled(
    const TServerSslContextConfigPtr& sslConfig)
{
    return sslConfig &&
        sslConfig->CertificateChain &&
        sslConfig->CertificateChain->FileName &&
        sslConfig->PrivateKey &&
        sslConfig->PrivateKey->FileName &&
        sslConfig->UpdatePeriod;
}

////////////////////////////////////////////////////////////////////////////////

class TSslCertificateUpdater
    : public ISslCertificateUpdater
{
public:
    TSslCertificateUpdater(
        IInvokerPtr controlInvoker,
        TSslContextPtr sslContext,
        TServerSslContextConfigPtr sslConfig,
        NLogging::TLogger logger)
        : SslContext_(std::move(sslContext))
        , SslConfig_(std::move(sslConfig))
        , Logger(std::move(logger))
        , Executor_(New<TPeriodicExecutor>(
            std::move(controlInvoker),
            BIND(&TSslCertificateUpdater::TryUpdate, MakeWeak(this)),
            SslConfig_->UpdatePeriod))
    { }

    void Start() override
    {
        Executor_->Start();
    }

    TFuture<void> Stop() override
    {
        return Executor_->Stop();
    }

private:
    const TSslContextPtr SslContext_;
    const TServerSslContextConfigPtr SslConfig_;
    const NLogging::TLogger Logger;
    const TPeriodicExecutorPtr Executor_;

    void TryUpdate()
    {
        try {
            auto modificationTime = Max(
                NFS::GetPathStatistics(*SslConfig_->CertificateChain->FileName).ModificationTime,
                NFS::GetPathStatistics(*SslConfig_->PrivateKey->FileName).ModificationTime);

            // Detect fresh and stable updates.
            if (modificationTime > SslContext_->GetCommitTime() &&
                modificationTime + *SslConfig_->UpdatePeriod <= TInstant::Now())
            {
                YT_TLOG_INFO("Updating TLS certificates")
                    .With("ModificationTime", modificationTime);
                SslContext_->Reset();
                SslContext_->ApplyConfig(SslConfig_);
                SslContext_->Commit(modificationTime);
                YT_TLOG_INFO("TLS certificates updated");
            }
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Unexpected exception while updating TLS certificates")
                .With(ex);
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISslCertificateUpdaterPtr CreateSslCertificateUpdater(
    IInvokerPtr controlInvoker,
    TSslContextPtr sslContext,
    TServerSslContextConfigPtr sslConfig,
    NLogging::TLogger logger)
{
    if (!IsSslCertificateUpdateEnabled(sslConfig)) {
        return nullptr;
    }

    YT_VERIFY(controlInvoker);
    return New<TSslCertificateUpdater>(
        std::move(controlInvoker),
        std::move(sslContext),
        std::move(sslConfig),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto

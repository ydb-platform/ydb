#include "tls_certificate_provider.h"

#include "tls_utils.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/digest/city.h>
#include <util/folder/dirut.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>

namespace NYdb::NBS {

using NYdb::NBS::FormatError;
using NYdb::NBS::HasError;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 RootCaFingerprint(TStringBuf rootCa)
{
    // Dynamic counters export gauges as double. Keep only 53 bits to avoid
    // precision loss while preserving a high-quality fingerprint.
    return CityHash64(rootCa) & ((1ULL << 53) - 1);
}

////////////////////////////////////////////////////////////////////////////////

class TStaticCertificateProvider final: public ICertificateProvider
{
private:
    const NTlsUtils::TRootCaPair RootCaPair;
    const ILoggingServicePtr Logging;
    const TString LogComponent;
    const NMonitoring::TDynamicCountersPtr ServerGroup;
    TVector<NTlsUtils::TCertificatePair> Certificates;

    TLog Log;

public:
    TStaticCertificateProvider(
        TString rootCertPath,
        TVector<TCertificateFiles> certificates,
        ILoggingServicePtr logging,
        TString logComponent,
        NMonitoring::TDynamicCountersPtr serverGroup)
        : RootCaPair(NTlsUtils::LoadRootCaPair(std::move(rootCertPath)))
        , Logging(std::move(logging))
        , LogComponent(std::move(logComponent))
        , ServerGroup(std::move(serverGroup))
        , Certificates(NTlsUtils::LoadCertificatePairs(std::move(certificates)))
    {}

    NThreading::TFuture<void> UpdateCertificates() override
    {
        return NThreading::MakeFuture();
    }

    std::shared_ptr<grpc::ChannelCredentials>
    CreateSecureClientCredentials() override
    {
        grpc::SslCredentialsOptions sslOptions{
            .pem_root_certs = RootCaPair.RootCa,
        };

        if (!Certificates.empty()) {
            const auto& cert = Certificates.front();
            sslOptions.pem_cert_chain = cert.CertChain;
            sslOptions.pem_private_key = cert.PrivateKey;
        }

        return grpc::SslCredentials(sslOptions);
    }

    std::shared_ptr<grpc::ServerCredentials>
    CreateSecureServerCredentials() override
    {
        grpc::SslServerCredentialsOptions sslOptions;

        sslOptions.client_certificate_request =
            GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY;

        sslOptions.pem_root_certs = RootCaPair.RootCa;

        for (const auto& cert: Certificates) {
            sslOptions.pem_key_cert_pairs.push_back({
                .cert_chain = cert.CertChain,
                .private_key = cert.PrivateKey,
            });
        }

        return grpc::SslServerCredentials(sslOptions);
    }

    void Start() override
    {
        if (Logging) {
            Log = Logging->CreateLog(LogComponent);
        }

        InitCounters();
    }

    void Stop() override
    {}

private:
    void InitCounters()
    {
        if (!ServerGroup) {
            return;
        }

        auto tlsMetricsGroup =
            ServerGroup->GetSubgroup("subsystem", "certificates");

        if (RootCaPair.RootCaPath) {
            auto rootMetrics = tlsMetricsGroup->GetSubgroup(
                "cert",
                GetBaseName(RootCaPair.RootCaPath));
            *rootMetrics->GetCounter("Fingerprint", false) =
                RootCaFingerprint(RootCaPair.RootCa);
        }

        for (const auto& cert: Certificates) {
            auto certMetrics = tlsMetricsGroup->GetSubgroup(
                "cert",
                GetBaseName(cert.Files.CertChainPath));

            auto expireTs = certMetrics->GetCounter("ExpireTs", false);

            auto [seconds, error] =
                NTlsUtils::GetCertificateNotAfterTimestampSec(cert.CertChain);
            if (HasError(error)) {
                STORAGE_WARN(
                    "Unable to parse certificate notAfter date for "
                    << cert.Files.CertChainPath.Quote() << ": "
                    << FormatError(error));
            } else {
                expireTs->Set(static_cast<TAtomicBase>(seconds));
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICertificateProviderPtr CreateStaticCertificateProvider(
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    ILoggingServicePtr logging,
    TString logComponent,
    NMonitoring::TDynamicCountersPtr serverGroup)
{
    return std::make_shared<TStaticCertificateProvider>(
        std::move(rootCertPath),
        std::move(certificates),
        std::move(logging),
        std::move(logComponent),
        std::move(serverGroup));
}

ICertificateProviderPtr CreateCertificateProviderStub()
{
    return CreateStaticCertificateProvider({}, {});
}

ICertificateProviderPtr CreateCertificateProvider(
    ILoggingServicePtr logging,
    TString logComponent,
    ISchedulerPtr scheduler,
    ITaskQueuePtr taskQueue,
    NMonitoring::TDynamicCountersPtr serverGroup,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshInterval)
{
    if (refreshInterval == TDuration::Zero()) {
        return CreateStaticCertificateProvider(
            std::move(rootCertPath),
            std::move(certificates),
            std::move(logging),
            std::move(logComponent),
            std::move(serverGroup));
    }

    auto certs =
        NTlsUtils::PrepareCertificateFilePairs(std::move(certificates));
    if (certs.empty()) {
        if (rootCertPath) {
            return CreatePeriodicCertificateProvider(
                std::move(logging),
                std::move(logComponent),
                std::move(scheduler),
                std::move(taskQueue),
                std::move(serverGroup),
                std::move(rootCertPath),
                {},
                refreshInterval);
        }

        return CreateStaticCertificateProvider({}, {});
    }

    return CreatePeriodicCertificateProvider(
        std::move(logging),
        std::move(logComponent),
        std::move(scheduler),
        std::move(taskQueue),
        std::move(serverGroup),
        std::move(rootCertPath),
        std::move(certs),
        refreshInterval);
}

ICertificateProviderPtr CreateStaticCertificateProvider(
    TString rootCertPath,
    TVector<TCertificateFiles> certificates)
{
    return CreateStaticCertificateProvider(
        std::move(rootCertPath),
        std::move(certificates),
        {},       // logging
        {},       // logComponent
        nullptr   // serverGroup
    );
}

}   // namespace NYdb::NBS

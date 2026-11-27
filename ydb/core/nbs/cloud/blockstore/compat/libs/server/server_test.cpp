#include "server_test.h"

#include "config.h"
#include "server.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/client/client.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/client/durable.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/config.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/request_stats.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/server_stats.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/volume_stats.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/monitoring.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/init.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/tls_certificate_provider.h>

#include <library/cpp/testing/unittest/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;

using NYdb::NBS::CreateCertificateProviderStub;
using NYdb::NBS::CreateLoggingService;
using NYdb::NBS::CreateMonitoringServiceStub;
using NYdb::NBS::CreateSchedulerStub;
using NYdb::NBS::CreateStaticCertificateProvider;
using NYdb::NBS::CreateWallClockTimer;
using NYdb::NBS::HasError;
using NYdb::NBS::ICertificateProviderPtr;
using NYdb::NBS::TCertificateFiles;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GetTestFilePath(const TString& fileName)
{
    return JoinFsPaths(
        ArcadiaSourceRoot(),
        "ydb/core/nbs/cloud/blockstore/tests",
        fileName);
}

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    return std::make_shared<TDiagnosticsConfig>();
}

ICertificateProviderPtr CreateServerCertificateProvider(
    const TServerAppConfigPtr& config,
    ILoggingServicePtr /*logging*/)
{
    TVector<TCertificateFiles> certPathList;
    for (const auto& cert: config->GetCerts()) {
        certPathList.push_back({
            cert.CertPrivateKeyFile,
            cert.CertFile
        });
    }

    if (certPathList.empty()) {
        certPathList.push_back({
            config->GetCertPrivateKeyFile(),
            config->GetCertFile()
        });
    }

    return CreateStaticCertificateProvider(
        config->GetRootCertsFile(),
        std::move(certPathList));
}

ICertificateProviderPtr CreateClientCertificateProvider(
    const TClientAppConfigPtr& config,
    ILoggingServicePtr /*logging*/)
{
    if (!config->GetSecurePort()) {
        return CreateCertificateProviderStub();
    }

    TVector<TCertificateFiles> certPathList {
        {
            .PrivateKeyPath = config->GetCertPrivateKeyFile(),
            .CertChainPath = config->GetCertFile()
        }
    };

    return CreateStaticCertificateProvider(
        config->GetRootCertsFile(),
        std::move(certPathList));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTestServerBuilder::TTestServerBuilder(TTestContext testContext)
    : TestContext(std::move(testContext))
{
}

TTestServerBuilder& TTestServerBuilder::SetPort(ui16 port)
{
    ServerAppConfig.MutableServerConfig()->SetPort(port);
    return *this;
}

TTestServerBuilder& TTestServerBuilder::SetDataPort(ui16 port)
{
    ServerAppConfig.MutableServerConfig()->SetDataPort(port);
    return *this;
}

TTestServerBuilder& TTestServerBuilder::SetSecureEndpoint(
    ui16 port,
    const TString& rootCertsFileName,
    const TString& certFileName,
    const TString& certPrivateKeyFileName)
{
    auto& serverConfig = *ServerAppConfig.MutableServerConfig();
    serverConfig.SetSecurePort(port);
    serverConfig.SetRootCertsFile(GetTestFilePath(rootCertsFileName));
    serverConfig.SetCertFile(GetTestFilePath(certFileName));
    serverConfig.SetCertPrivateKeyFile(GetTestFilePath(certPrivateKeyFileName));
    return *this;
}

TTestServerBuilder& TTestServerBuilder::AddCert(
    const TString& certFileName,
    const TString& certPrivateKeyFileName)
{
    auto& cert = *ServerAppConfig.MutableServerConfig()->AddCerts();
    cert.SetCertFile(GetTestFilePath(certFileName));
    cert.SetCertPrivateKeyFile(GetTestFilePath(certPrivateKeyFileName));
    return *this;
}

TTestServerBuilder& TTestServerBuilder::SetUnixSocketPath(const TString& unixSocketPath)
{
    ServerAppConfig.MutableServerConfig()->SetUnixSocketPath(unixSocketPath);
    return *this;
}

TTestServerBuilder& TTestServerBuilder::SetVolumeStats(IVolumeStatsPtr volumeStats)
{
    TestContext.VolumeStats = std::move(volumeStats);
    return *this;
}

TTestServerBuilder& TTestServerBuilder::SetCellId(TString cellId)
{
    TestContext.CellId = std::move(cellId);
    return *this;
}

IServerPtr TTestServerBuilder::BuildServer(
    IBlockStorePtr service,
    IBlockStorePtr udsService)
{
    auto serverConfig = std::make_shared<TServerAppConfig>(ServerAppConfig);

    auto serverStats = CreateServerStats(
        serverConfig,
        CreateTestDiagnosticsConfig(),
        TestContext.Monitoring,
        TestContext.RequestStats,
        TestContext.VolumeStats);
    auto certificateProvider = CreateServerCertificateProvider(
        serverConfig,
        TestContext.Logging);

    auto server = CreateServer(
        std::move(serverConfig),
        TestContext.Logging,
        std::move(serverStats),
        std::move(service),
        std::move(udsService),
        TServerOptions {
            .CellId = TestContext.CellId
        },
        std::move(certificateProvider));
    return server;
}

////////////////////////////////////////////////////////////////////////////////

TTestClientBuilder::TTestClientBuilder(TTestContext testContext)
    : TestContext(std::move(testContext))
{
    auto& clientConfig = *ClientAppConfig.MutableClientConfig();
    clientConfig.SetClientId(CreateGuidAsString());
}

TTestClientBuilder& TTestClientBuilder::SetPort(ui16 port)
{
    ClientAppConfig.MutableClientConfig()->SetInsecurePort(port);
    return *this;
}

TTestClientBuilder& TTestClientBuilder::SetDataPort(ui16 port)
{
    ClientAppConfig.MutableClientConfig()->SetPort(port);
    return *this;
}

TTestClientBuilder& TTestClientBuilder::SetClientId(const TString& clientId)
{
    auto& clientConfig = *ClientAppConfig.MutableClientConfig();
    clientConfig.SetClientId(clientId);
    return *this;
}

TTestClientBuilder& TTestClientBuilder::SetSecureEndpoint(
    ui16 port,
    const TString& rootCertsFileName,
    const TString& authToken)
{
    auto& clientConfig = *ClientAppConfig.MutableClientConfig();
    clientConfig.SetSecurePort(port);
    clientConfig.SetRootCertsFile(GetTestFilePath(rootCertsFileName));
    clientConfig.SetAuthToken(authToken);
    return *this;
}

TTestClientBuilder& TTestClientBuilder::SetCertificate(
    const TString& certsFileName,
    const TString& certPrivateKeyFileName)
{
    auto& clientConfig = *ClientAppConfig.MutableClientConfig();
    clientConfig.SetCertFile(GetTestFilePath(certsFileName));
    clientConfig.SetCertPrivateKeyFile(GetTestFilePath(certPrivateKeyFileName));
    return *this;
}

TTestClientBuilder& TTestClientBuilder::SetUnixSocketPath(const TString& unixSocketPath)
{
    ClientAppConfig.MutableClientConfig()->SetUnixSocketPath(unixSocketPath);
    return *this;
}

TTestClientBuilder& TTestClientBuilder::SetVolumeStats(IVolumeStatsPtr volumeStats)
{
    TestContext.VolumeStats = std::move(volumeStats);
    return *this;
}

IClientPtr TTestClientBuilder::BuildClient()
{
    auto clientConfig = std::make_shared<TClientAppConfig>(ClientAppConfig);

    auto clientStats = CreateClientStats(
        clientConfig,
        TestContext.Monitoring,
        TestContext.RequestStats,
        TestContext.VolumeStats,
        clientConfig->GetInstanceId());
    auto certificateProvider = CreateClientCertificateProvider(
        clientConfig,
        TestContext.Logging);

    auto [client, error] = CreateClient(
        std::move(clientConfig),
        TestContext.Timer,
        TestContext.Scheduler,
        TestContext.Logging,
        TestContext.Monitoring,
        std::move(clientStats),
        std::move(certificateProvider));

    UNIT_ASSERT(!HasError(error));
    return client;
}

////////////////////////////////////////////////////////////////////////////////

TTestFactory::TTestFactory()
{
    Timer = CreateWallClockTimer();
    Scheduler = CreateSchedulerStub();
    Logging = CreateLoggingService("console");
    Monitoring = CreateMonitoringServiceStub();
    RequestStats = CreateRequestStatsStub();
    VolumeStats = CreateVolumeStatsStub();
}

TTestServerBuilder TTestFactory::CreateServerBuilder()
{
    return TTestServerBuilder(*this);
}

TTestClientBuilder TTestFactory::CreateClientBuilder()
{
    return TTestClientBuilder(*this);
}

IBlockStorePtr TTestFactory::CreateDurableClient(IBlockStorePtr client)
{
    auto config = std::make_shared<TClientAppConfig>();
    return NClient::CreateDurableClient(
        config,
        std::move(client),
        CreateRetryPolicy(config, std::nullopt),
        Logging,
        Timer,
        Scheduler,
        RequestStats,
        VolumeStats);
}

}   // namespace NCloud::NBlockStore::NServer

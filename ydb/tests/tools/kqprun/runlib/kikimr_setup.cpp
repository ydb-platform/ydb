#include "kikimr_setup.h"
#include "utils.h"

#include <util/system/hostname.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/grpc/server/actors/logger.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <yt/yql/providers/yt/mkql_dq/yql_yt_dq_transform.h>

namespace NKikimrRun {

namespace {

class TStaticCredentialsProvider : public NYdb::ICredentialsProvider {
public:
    TStaticCredentialsProvider(const TString& yqlToken)
        : YqlToken_(yqlToken)
    {}

    std::string GetAuthInfo() const override {
        return YqlToken_;
    }

    bool IsValid() const override {
        return true;
    }

private:
    std::string YqlToken_;
};

class TStaticCredentialsProviderFactory : public NYdb::ICredentialsProviderFactory {
public:
    TStaticCredentialsProviderFactory(const TString& yqlToken)
        : YqlToken_(yqlToken)
    {}

    std::shared_ptr<NYdb::ICredentialsProvider> CreateProvider() const override {
        return std::make_shared<TStaticCredentialsProvider>(YqlToken_);
    }

private:
    TString YqlToken_;
};

class TStaticSecuredCredentialsFactory : public NYql::ISecuredServiceAccountCredentialsFactory {
public:
    TStaticSecuredCredentialsFactory(const TString& yqlToken)
        : YqlToken_(yqlToken)
    {}

    std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString&, const TString&) override {
        return std::make_shared<TStaticCredentialsProviderFactory>(YqlToken_);
    }

private:
    TString YqlToken_;
};

}  // anonymous namespace

TAutoPtr<TLogBackend> TKikimrSetupBase::CreateLogBackend(const TServerSettings& settings) const {
    if (settings.LogOutputFile) {
        return NActors::CreateFileBackend(settings.LogOutputFile);
    } else {
        return NActors::CreateStderrBackend();
    }
}

NKikimr::Tests::TServerSettings TKikimrSetupBase::GetServerSettings(const TServerSettings& settings, ui32 grpcPort, bool verbosity) {
    const ui32 msgBusPort = PortManager.GetPort();

    NKikimr::Tests::TServerSettings serverSettings(msgBusPort, settings.AppConfig.GetAuthConfig(), settings.AppConfig.GetPQConfig());
    serverSettings.SetNodeCount(settings.NodeCount);

    serverSettings.SetDomainName(TString(NKikimr::ExtractDomain(NKikimr::CanonizePath(settings.DomainName))));
    serverSettings.SetAppConfig(settings.AppConfig);
    serverSettings.SetFeatureFlags(settings.AppConfig.GetFeatureFlags());
    serverSettings.SetControls(settings.AppConfig.GetImmediateControlsConfig());
    serverSettings.SetCompactionConfig(settings.AppConfig.GetCompactionConfig());
    serverSettings.PQClusterDiscoveryConfig = settings.AppConfig.GetPQClusterDiscoveryConfig();
    serverSettings.NetClassifierConfig = settings.AppConfig.GetNetClassifierConfig();

    const auto& kqpSettings = settings.AppConfig.GetKQPConfig().GetSettings();
    serverSettings.SetKqpSettings({kqpSettings.begin(), kqpSettings.end()});

    serverSettings.SetCredentialsFactory(std::make_shared<TStaticSecuredCredentialsFactory>(settings.YqlToken));
    serverSettings.SetComputationFactory(settings.ComputationFactory);
    serverSettings.SetYtGateway(settings.YtGateway);
    serverSettings.S3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
    serverSettings.SetDqTaskTransformFactory(NYql::CreateYtDqTaskTransformFactory(true));
    serverSettings.SetInitializeFederatedQuerySetupFactory(true);
    serverSettings.SetVerbose(verbosity);
    serverSettings.SetNeedStatsCollectors(true);

    SetLoggerSettings(settings, serverSettings);
    SetFunctionRegistry(settings, serverSettings);

    if (settings.MonitoringEnabled) {
        serverSettings.InitKikimrRunConfig();
        serverSettings.SetMonitoringPortOffset(settings.FirstMonitoringPort, true);
    }

    if (settings.GrpcEnabled) {
        serverSettings.SetGrpcPort(grpcPort);
        serverSettings.SetGrpcHost(HostName());
    }

    return serverSettings;
}

NYdbGrpc::TServerOptions TKikimrSetupBase::GetGrpcSettings(ui32 grpcPort, ui32 nodeIdx, TDuration shutdownDeadline) const {
    return NYdbGrpc::TServerOptions()
        .SetHost("[::]")
        .SetPort(grpcPort)
        .SetGRpcShutdownDeadline(shutdownDeadline)
        .SetLogger(NYdbGrpc::CreateActorSystemLogger(*GetRuntime()->GetActorSystem(nodeIdx), NKikimrServices::GRPC_SERVER));
}

std::optional<NKikimrWhiteboard::TSystemStateInfo> TKikimrSetupBase::GetSystemStateInfo(TIntrusivePtr<NKikimr::NMemory::IProcessMemoryInfoProvider> memoryInfoProvider) {
    if (!memoryInfoProvider) {
        return std::nullopt;
    }

    NKikimrWhiteboard::TSystemStateInfo systemStateInfo;

    const auto& memInfo = memoryInfoProvider->Get();
    if (memInfo.CGroupLimit) {
        systemStateInfo.SetMemoryLimit(*memInfo.CGroupLimit);
    } else if (memInfo.MemTotal) {
        systemStateInfo.SetMemoryLimit(*memInfo.MemTotal);
    }

    return systemStateInfo;
}

TString TKikimrSetupBase::FormatMonitoringLink(ui16 port, const TString& uri) {
    return TStringBuilder() << port << " (view link: http://" << HostName() << ":" << port << "/" << uri << ")";
}

TString TKikimrSetupBase::FormatGrpcLink(ui16 port) {
    return TStringBuilder() << port << " (connection: grpc://" << HostName() << ":" << port << ")";
}

void TKikimrSetupBase::SetLoggerSettings(const TServerSettings& settings, NKikimr::Tests::TServerSettings& serverSettings) const {
    auto loggerInitializer = [this, settings](NActors::TTestActorRuntime& runtime) {
        InitLogSettings(settings.AppConfig.GetLogConfig(), runtime);
        runtime.SetLogBackendFactory([this, settings]() { return CreateLogBackend(settings); });
    };

    serverSettings.SetLoggerInitializer(loggerInitializer);
}

void TKikimrSetupBase::SetFunctionRegistry(const TServerSettings& settings, NKikimr::Tests::TServerSettings& serverSettings) const {
    if (!settings.FunctionRegistry) {
        return;
    }

    auto functionRegistryFactory = [settings](const NKikimr::NScheme::TTypeRegistry&) {
        return settings.FunctionRegistry.Get();
    };

    serverSettings.SetFrFactory(functionRegistryFactory);
}

}  // namespace NKikimrRun

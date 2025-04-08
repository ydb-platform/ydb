#include "kikimr_setup.h"
#include "utils.h"

#include <ydb/library/actors/core/log.h>
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

TKikimrSetupBase::TKikimrSetupBase(const TServerSettings& settings)
    : Settings(settings)
{}

TAutoPtr<TLogBackend> TKikimrSetupBase::CreateLogBackend() const {
    if (Settings.LogOutputFile) {
        return NActors::CreateFileBackend(Settings.LogOutputFile);
    } else {
        return NActors::CreateStderrBackend();
    }
}

NKikimr::Tests::TServerSettings TKikimrSetupBase::GetServerSettings(ui32 grpcPort, bool verbose) {
    const ui32 msgBusPort = PortManager.GetPort();

    NKikimr::Tests::TServerSettings serverSettings(msgBusPort, Settings.AppConfig.GetAuthConfig(), Settings.AppConfig.GetPQConfig());
    serverSettings.SetNodeCount(Settings.NodeCount);

    serverSettings.SetDomainName(TString(NKikimr::ExtractDomain(NKikimr::CanonizePath(Settings.DomainName))));
    serverSettings.SetAppConfig(Settings.AppConfig);
    serverSettings.SetFeatureFlags(Settings.AppConfig.GetFeatureFlags());
    serverSettings.SetControls(Settings.AppConfig.GetImmediateControlsConfig());
    serverSettings.SetCompactionConfig(Settings.AppConfig.GetCompactionConfig());
    serverSettings.PQClusterDiscoveryConfig = Settings.AppConfig.GetPQClusterDiscoveryConfig();
    serverSettings.NetClassifierConfig = Settings.AppConfig.GetNetClassifierConfig();

    const auto& kqpSettings = Settings.AppConfig.GetKQPConfig().GetSettings();
    serverSettings.SetKqpSettings({kqpSettings.begin(), kqpSettings.end()});

    serverSettings.SetCredentialsFactory(std::make_shared<TStaticSecuredCredentialsFactory>(Settings.YqlToken));
    serverSettings.SetComputationFactory(Settings.ComputationFactory);
    serverSettings.SetYtGateway(Settings.YtGateway);
    serverSettings.S3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
    serverSettings.SetDqTaskTransformFactory(NYql::CreateYtDqTaskTransformFactory(true));
    serverSettings.SetInitializeFederatedQuerySetupFactory(true);
    serverSettings.SetVerbose(verbose);
    serverSettings.SetNeedStatsCollectors(true);

    SetLoggerSettings(serverSettings);
    SetFunctionRegistry(serverSettings);

    if (Settings.MonitoringEnabled) {
        serverSettings.InitKikimrRunConfig();
        serverSettings.SetMonitoringPortOffset(Settings.FirstMonitoringPort, true);
    }

    if (Settings.GrpcEnabled) {
        serverSettings.SetGrpcPort(grpcPort);
    }

    return serverSettings;
}

void TKikimrSetupBase::SetLoggerSettings(NKikimr::Tests::TServerSettings& serverSettings) const {
    auto loggerInitializer = [this](NActors::TTestActorRuntime& runtime) {
        InitLogSettings(Settings.AppConfig.GetLogConfig(), runtime);
        runtime.SetLogBackendFactory([this]() { return CreateLogBackend(); });
    };

    serverSettings.SetLoggerInitializer(loggerInitializer);
}

void TKikimrSetupBase::SetFunctionRegistry(NKikimr::Tests::TServerSettings& serverSettings) const {
    if (!Settings.FunctionRegistry) {
        return;
    }

    auto functionRegistryFactory = [this](const NKikimr::NScheme::TTypeRegistry&) {
        return Settings.FunctionRegistry.Get();
    };

    serverSettings.SetFrFactory(functionRegistryFactory);
}

}  // namespace NKikimrRun

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

TAutoPtr<TLogBackend> TKikimrSetupBase::CreateLogBackend(const TServerSettings& settings) const {
    if (settings.LogOutputFile) {
        return NActors::CreateFileBackend(settings.LogOutputFile);
    } else {
        return NActors::CreateStderrBackend();
    }
}

NKikimr::Tests::TServerSettings TKikimrSetupBase::GetServerSettings(const TServerSettings& settings, ui32 grpcPort, bool verbose) {
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
    serverSettings.SetVerbose(verbose);
    serverSettings.SetNeedStatsCollectors(true);

    SetLoggerSettings(settings, serverSettings);
    SetFunctionRegistry(settings, serverSettings);

    if (settings.MonitoringEnabled) {
        serverSettings.InitKikimrRunConfig();
        serverSettings.SetMonitoringPortOffset(settings.FirstMonitoringPort, true);
    }

    if (settings.GrpcEnabled) {
        serverSettings.SetGrpcPort(grpcPort);
    }

    return serverSettings;
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

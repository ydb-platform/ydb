#include "auth_factory.h"
#include "http_req.h"
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/http_proxy/http_service.h>
#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/http_proxy/metrics_actor.h>
#include <ydb/core/http_proxy/discovery_actor.h>

#include <ydb/public/sdk/cpp/client/iam_private/iam.h>

#include <ydb/library/actors/http/http_proxy.h>

#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NHttpProxy {


void TIamAuthFactory::InitTenantDiscovery(
    NActors::TActorSystemSetup::TLocalServices&,
    const TAppData&,
    const THttpConfig&, ui16)
{
}

void TIamAuthFactory::Initialize(
    NActors::TActorSystemSetup::TLocalServices& localServices,
    const TAppData& appData,
    const THttpConfig& httpConfig,
    const  NKikimrConfig::TGRpcConfig& grpcConfig)
{
    if (!httpConfig.GetEnabled()) {
        return;
    }

    ui32 grpcPort = 0;
    TString CA;
    grpcPort = grpcConfig.GetPort();
    if (!grpcPort) {
        grpcPort = grpcConfig.GetSslPort();
    }
    CA = httpConfig.GetCA();

    NKikimrConfig::TServerlessProxyConfig config;
    config.MutableHttpConfig()->CopyFrom(httpConfig);
    config.SetCaCert(CA);
    if (httpConfig.GetYandexCloudServiceRegion().size() == 0) {
        ythrow yexception() << "YandexCloudServiceRegion must not be empty";
    }
    IActor* actor = NKikimr::NHttpProxy::CreateAccessServiceActor(config);
    localServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            NKikimr::NHttpProxy::MakeAccessServiceID(),
            TActorSetupCmd(actor, TMailboxType::HTSwap, appData.UserPoolId)));

    InitTenantDiscovery(localServices, appData, httpConfig, grpcPort);

    const TString& jwtFilename = httpConfig.GetJwtFile();
    TString iamExternalEndpoint = httpConfig.GetIamTokenServiceEndpoint();

    const NYdb::TCredentialsProviderFactoryPtr credentialsProviderFactory = jwtFilename.empty()
        ? NYdb::CreateInsecureCredentialsProviderFactory()
        : NYdb::CreateIamJwtFileCredentialsProviderFactoryPrivate(
            {{.Endpoint = iamExternalEndpoint}, jwtFilename} );
    const NYdb::TCredentialsProviderPtr credentialsProvider = credentialsProviderFactory->CreateProvider();


    actor = NKikimr::NHttpProxy::CreateIamTokenServiceActor(config);
    localServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            NKikimr::NHttpProxy::MakeIamTokenServiceID(),
            TActorSetupCmd(actor, TMailboxType::HTSwap, appData.UserPoolId)));

    bool isServerless = appData.FeatureFlags.GetEnableDbCounters(); //TODO: find out it via describe

    actor = NKikimr::NHttpProxy::CreateMetricsActor(NKikimr::NHttpProxy::TMetricsSettings{appData.Counters->GetSubgroup("counters", isServerless ? "http_proxy_serverless" : "http_proxy")});
    localServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            NKikimr::NHttpProxy::MakeMetricsServiceID(),
            TActorSetupCmd(actor, TMailboxType::HTSwap, appData.UserPoolId)));

    NKikimr::NHttpProxy::THttpProxyConfig httpProxyConfig;
    httpProxyConfig.Config = config;
    httpProxyConfig.CredentialsProvider = credentialsProvider;
    httpProxyConfig.UseSDK = UseSDK();

    actor = NKikimr::NHttpProxy::CreateHttpProxy(httpProxyConfig);
    localServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            NKikimr::NHttpProxy::MakeHttpProxyID(),
            TActorSetupCmd(actor, TMailboxType::HTSwap, appData.UserPoolId)));

    actor = NHttp::CreateHttpProxy();
    localServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            NKikimr::NHttpProxy::MakeHttpServerServiceID(),
            TActorSetupCmd(actor, TMailboxType::HTSwap, appData.UserPoolId)));
}

NActors::IActor* TIamAuthFactory::CreateAuthActor(const TActorId sender, THttpRequestContext& context, THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature) const
{
    return CreateIamAuthActor(sender, context, std::move(signature));
}


}


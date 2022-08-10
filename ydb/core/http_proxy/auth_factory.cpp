#include "auth_factory.h"
#include "http_service.h"
#include "http_req.h"
#include "metrics_actor.h"
#include "discovery_actor.h"

#include <library/cpp/actors/http/http_proxy.h>

namespace NKikimr::NHttpProxy {

void TAuthFactory::Initialize(
    NActors::TActorSystemSetup::TLocalServices& localServices,
    const TAppData& appData,
    const THttpConfig& httpConfig,
    const  NKikimrConfig::TGRpcConfig& grpcConfig)
{
    if (httpConfig.GetEnabled()) {
        ui32 grpcPort = 0;
        TString CA;
        grpcPort = grpcConfig.GetPort();
        // bool secure = false;
        if (!grpcPort) {
            // secure = true;
            grpcPort = grpcConfig.GetSslPort();
        }
        CA = grpcConfig.GetCA();

        NKikimrConfig::TServerlessProxyConfig config;
        config.MutableHttpConfig()->CopyFrom(httpConfig);
        config.SetCaCert(CA);
        if (httpConfig.GetYandexCloudServiceRegion().size() == 0) {
            ythrow yexception() << "YandexCloudServiceRegion must not be empty";
        }

        const NYdb::TCredentialsProviderPtr credentialsProvider = NYdb::CreateInsecureCredentialsProviderFactory()->CreateProvider();

        auto actor = NKikimr::NHttpProxy::CreateMetricsActor(NKikimr::NHttpProxy::TMetricsSettings{appData.Counters->GetSubgroup("counters", "http_proxy")});
        localServices.push_back(std::pair<TActorId, TActorSetupCmd>(
                NKikimr::NHttpProxy::MakeMetricsServiceID(),
                TActorSetupCmd(actor, TMailboxType::HTSwap, appData.UserPoolId)));

        NKikimr::NHttpProxy::THttpProxyConfig httpProxyConfig;
        httpProxyConfig.Config = config;
        httpProxyConfig.CredentialsProvider = credentialsProvider;

        actor = NKikimr::NHttpProxy::CreateHttpProxy(httpProxyConfig);
        localServices.push_back(std::pair<TActorId, TActorSetupCmd>(
                NKikimr::NHttpProxy::MakeHttpProxyID(),
                TActorSetupCmd(actor, TMailboxType::HTSwap, appData.UserPoolId)));

        actor = NHttp::CreateHttpProxy();
        localServices.push_back(std::pair<TActorId, TActorSetupCmd>(
                NKikimr::NHttpProxy::MakeHttpServerServiceID(),
                TActorSetupCmd(actor, TMailboxType::HTSwap, appData.UserPoolId)));
    }



}

NActors::IActor* TAuthFactory::CreateAuthActor(const TActorId, THttpRequestContext&, THolder<NKikimr::NSQS::TAwsRequestSignV4>&&) const
{
    Y_FAIL("No signature support");
    return nullptr;
}


}


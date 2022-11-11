#pragma once

#include "http_req.h"
#include <ydb/library/http_proxy/authorization/signature.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr::NHttpProxy {


class IAuthFactory {
public:
    using THttpConfig = NKikimrConfig::THttpProxyConfig;

    virtual void Initialize(
        NActors::TActorSystemSetup::TLocalServices& services,
        const TAppData& appData,
        const THttpConfig& config,
        const NKikimrConfig::TGRpcConfig& grpcConfig) = 0;

    virtual NActors::IActor* CreateAuthActor(const TActorId sender, THttpRequestContext& context, THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature) const = 0;

    virtual ~IAuthFactory() = default;
};


class TIamAuthFactory : public IAuthFactory {
    using THttpConfig = NKikimrConfig::THttpProxyConfig;

public:
    void Initialize(
        NActors::TActorSystemSetup::TLocalServices&,
        const TAppData& appData,
        const THttpConfig& config,
        const NKikimrConfig::TGRpcConfig& grpcConfig) final;

    NActors::IActor* CreateAuthActor(const TActorId sender, THttpRequestContext& context, THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature) const final;

    virtual void InitTenantDiscovery(NActors::TActorSystemSetup::TLocalServices&,
        const TAppData& appData,
        const THttpConfig& config, ui16 grpcPort);

    virtual bool UseSDK() const { return false; }
};

}


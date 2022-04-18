#pragma once

#include "http_req.h"
#include <ydb/library/http_proxy/authorization/signature.h>
#include <ydb/core/base/appdata.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

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

class TAuthFactory : public IAuthFactory {
public:
    void Initialize(
        NActors::TActorSystemSetup::TLocalServices&,
        const TAppData& appData,
        const THttpConfig& config,
        const NKikimrConfig::TGRpcConfig& grpcConfig) final;

    NActors::IActor* CreateAuthActor(const TActorId sender, THttpRequestContext& context, THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature) const final;
};

}

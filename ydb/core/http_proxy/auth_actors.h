#pragma once

#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NHttpProxy {
    struct THttpRequestContext;

    NActors::IActor* CreateAccessServiceActor(const NKikimrConfig::TServerlessProxyConfig& config);
    NActors::IActor* CreateIamTokenServiceActor(const NKikimrConfig::TServerlessProxyConfig& config);
    NActors::IActor* CreateIamAuthActor(const NActors::TActorId sender, THttpRequestContext& context, THolder<NKikimr::NSQS::TAwsRequestSignV4> signature);
}

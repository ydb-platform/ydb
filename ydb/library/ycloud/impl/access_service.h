#pragma once
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TAccessServiceSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateAccessServiceV1(const TAccessServiceSettings& settings);
IActor* CreateAccessServiceV2(const TAccessServiceSettings& settings);

inline IActor* CreateAccessServiceV1(TString endpoint, TString userAgentHint) {
    TAccessServiceSettings settings;
    settings.Endpoint = std::move(endpoint);
    settings.UserAgentHint = std::move(userAgentHint);
    return CreateAccessServiceV1(settings);
}

inline IActor* CreateAccessServiceV2(TString endpoint, TString userAgentHint) {
    TAccessServiceSettings settings;
    settings.Endpoint = std::move(endpoint);
    settings.UserAgentHint = std::move(userAgentHint);
    return CreateAccessServiceV2(settings);
}

IActor* CreateAccessServiceWithCache(const TAccessServiceSettings& settings, bool enableV2Interface); // for compatibility with older code

}

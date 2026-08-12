#pragma once
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TAccessServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    TAccessServiceSettings(TString endpoint, TString userAgentPrefix) {
        Endpoint = std::move(endpoint);
        UserAgentPrefix = std::move(userAgentPrefix);
    }
};

IActor* CreateAccessServiceV1(const TAccessServiceSettings& settings);
IActor* CreateAccessServiceV2(const TAccessServiceSettings& settings);

inline IActor* CreateAccessServiceV1(TString endpoint, TString userAgentPrefix) {
    TAccessServiceSettings settings(std::move(endpoint), std::move(userAgentPrefix));
    return CreateAccessServiceV1(settings);
}

inline IActor* CreateAccessServiceV2(TString endpoint, TString userAgentPrefix) {
    TAccessServiceSettings settings(std::move(endpoint), std::move(userAgentPrefix));
    return CreateAccessServiceV2(settings);
}

IActor* CreateAccessServiceWithCache(const TAccessServiceSettings& settings, bool enableV2Interface); // for compatibility with older code

}

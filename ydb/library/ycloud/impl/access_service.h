#pragma once

#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TAccessServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    TAccessServiceSettings(TString endpoint, TStringBuf userAgentHint);
};

IActor* CreateAccessServiceV1(const TAccessServiceSettings& settings);
IActor* CreateAccessServiceV2(const TAccessServiceSettings& settings);

inline IActor* CreateAccessServiceV1(TString endpoint, TStringBuf userAgentHint) {
    TAccessServiceSettings settings(std::move(endpoint), userAgentHint);
    return CreateAccessServiceV1(settings);
}

inline IActor* CreateAccessServiceV2(TString endpoint, TStringBuf userAgentHint) {
    TAccessServiceSettings settings(std::move(endpoint), userAgentHint);
    return CreateAccessServiceV2(settings);
}

IActor* CreateAccessServiceWithCache(const TAccessServiceSettings& settings, bool enableV2Interface); // for compatibility with older code

}

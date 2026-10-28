#pragma once

#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TAccessServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    TAccessServiceSettings(TString endpoint, TStringBuf userAgentHint);
};

IActor* CreateAccessService(const TAccessServiceSettings& settings);

inline IActor* CreateAccessService(TString endpoint, TStringBuf userAgentHint) {
    TAccessServiceSettings settings(std::move(endpoint), userAgentHint);
    return CreateAccessService(settings);
}

IActor* CreateAccessServiceWithCache(const TAccessServiceSettings& settings); // for compatibility with older code

}

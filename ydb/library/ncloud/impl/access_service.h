#pragma once
#include <ydb/library/ncloud/api/access_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NNebiusCloud {

using namespace NKikimr;

struct TAccessServiceSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateAccessServiceV1(const TAccessServiceSettings& settings);

inline IActor* CreateAccessServiceV1(TString endpoint, TString userAgentHint) {
    TAccessServiceSettings settings;
    settings.Endpoint = std::move(endpoint);
    settings.UserAgentHint = std::move(userAgentHint);
    return CreateAccessServiceV1(settings);
}

}

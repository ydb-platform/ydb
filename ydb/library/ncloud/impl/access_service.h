#pragma once
#include <ydb/library/ncloud/api/access_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NNebiusCloud {

using namespace NKikimr;

struct TAccessServiceSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateAccessServiceV1(const TAccessServiceSettings& settings);

inline IActor* CreateAccessServiceV1(const TString& endpoint) {
    TAccessServiceSettings settings;
    settings.Endpoint = endpoint;
    return CreateAccessServiceV1(settings);
}

}

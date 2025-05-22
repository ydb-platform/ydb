#pragma once
#include <ydb/library/ycloud/api/iam_token_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TIamTokenServiceSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateIamTokenService(const TIamTokenServiceSettings& settings);

inline IActor* CreateIamTokenService(const TString& endpoint) {
    TIamTokenServiceSettings settings;
    settings.Endpoint = endpoint;
    return CreateIamTokenService(settings);
}

IActor* CreateIamTokenServiceWithCache(const TIamTokenServiceSettings& settings); // for compatibility with older code

}

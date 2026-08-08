#pragma once
#include <ydb/library/ycloud/api/iam_token_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TIamTokenServiceSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateIamTokenService(const TIamTokenServiceSettings& settings);

inline IActor* CreateIamTokenService(TString endpoint, TString userAgentHint) {
    TIamTokenServiceSettings settings;
    settings.Endpoint = std::move(endpoint);
    settings.UserAgentHint = std::move(userAgentHint);
    return CreateIamTokenService(settings);
}

IActor* CreateIamTokenServiceWithCache(const TIamTokenServiceSettings& settings); // for compatibility with older code

}

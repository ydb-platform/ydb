#pragma once
#include <ydb/library/ycloud/api/iam_token_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TIamTokenServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    TIamTokenServiceSettings(TString endpoint, TString userAgentPrefix) {
        Endpoint = std::move(endpoint);
        UserAgentPrefix = std::move(userAgentPrefix);
    }
};

IActor* CreateIamTokenService(const TIamTokenServiceSettings& settings);

inline IActor* CreateIamTokenService(TString endpoint, TString userAgentPrefix) {
    TIamTokenServiceSettings settings(std::move(endpoint), std::move(userAgentPrefix));
    return CreateIamTokenService(settings);
}

IActor* CreateIamTokenServiceWithCache(const TIamTokenServiceSettings& settings); // for compatibility with older code

}

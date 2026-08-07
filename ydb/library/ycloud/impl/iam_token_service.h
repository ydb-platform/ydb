#pragma once
#include <ydb/library/ycloud/api/iam_token_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TIamTokenServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    using NGrpcActorClient::TGrpcClientSettings::TGrpcClientSettings;
};

IActor* CreateIamTokenService(const TIamTokenServiceSettings& settings);

inline IActor* CreateIamTokenService(const TString& endpoint, const TString& userAgentHint) {
    TIamTokenServiceSettings settings(userAgentHint);
    settings.Endpoint = endpoint;
    return CreateIamTokenService(settings);
}

IActor* CreateIamTokenServiceWithCache(const TIamTokenServiceSettings& settings); // for compatibility with older code

}

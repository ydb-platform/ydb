#pragma once
#include <ydb/library/ycloud/api/service_account_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {

using namespace NKikimr;

struct TServiceAccountServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    explicit TServiceAccountServiceSettings(TString endpoint, TString userAgentPrefix) {
        Endpoint = std::move(endpoint);
        UserAgentPrefix = std::move(userAgentPrefix);
    }
};

IActor* CreateServiceAccountService(const TServiceAccountServiceSettings& settings);

inline IActor* CreateServiceAccountService(TString endpoint, TString userAgentPrefix) {
    TServiceAccountServiceSettings settings(std::move(endpoint), std::move(userAgentPrefix));
    return CreateServiceAccountService(settings);
}

}

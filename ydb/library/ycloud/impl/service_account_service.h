#pragma once
#include <ydb/library/ycloud/api/service_account_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {

using namespace NKikimr;

struct TServiceAccountServiceSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateServiceAccountService(const TServiceAccountServiceSettings& settings);

inline IActor* CreateServiceAccountService(TString endpoint, TString userAgentHint) {
    TServiceAccountServiceSettings settings;
    settings.Endpoint = std::move(endpoint);
    settings.UserAgentHint = std::move(userAgentHint);
    return CreateServiceAccountService(settings);
}

}

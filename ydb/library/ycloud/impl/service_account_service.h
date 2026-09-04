#pragma once
#include <ydb/library/ycloud/api/service_account_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {

using namespace NKikimr;

struct TServiceAccountServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    TServiceAccountServiceSettings(TString endpoint, TStringBuf userAgentHint);
};

IActor* CreateServiceAccountService(const TServiceAccountServiceSettings& settings);

inline IActor* CreateServiceAccountService(TString endpoint, TStringBuf userAgentHint) {
    TServiceAccountServiceSettings settings(std::move(endpoint), userAgentHint);
    return CreateServiceAccountService(settings);
}

}

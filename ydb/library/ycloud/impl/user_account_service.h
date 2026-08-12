#pragma once
#include <ydb/library/ycloud/api/user_account_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {

using namespace NKikimr;

struct TUserAccountServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    TUserAccountServiceSettings(TString endpoint, TString userAgentPrefix) {
        Endpoint = std::move(endpoint);
        UserAgentPrefix = std::move(userAgentPrefix);
    }
};

IActor* CreateUserAccountService(const TUserAccountServiceSettings& settings);

inline IActor* CreateUserAccountService(TString endpoint, TString userAgentPrefix) {
    TUserAccountServiceSettings settings(std::move(endpoint), std::move(userAgentPrefix));
    return CreateUserAccountService(settings);
}

}

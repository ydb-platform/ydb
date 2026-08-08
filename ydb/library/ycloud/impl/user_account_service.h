#pragma once
#include <ydb/library/ycloud/api/user_account_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {

using namespace NKikimr;

struct TUserAccountServiceSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateUserAccountService(const TUserAccountServiceSettings& settings);

inline IActor* CreateUserAccountService(TString endpoint, TString userAgentHint) {
    TUserAccountServiceSettings settings;
    settings.Endpoint = std::move(endpoint);
    settings.UserAgentHint = std::move(userAgentHint);
    return CreateUserAccountService(settings);
}

}

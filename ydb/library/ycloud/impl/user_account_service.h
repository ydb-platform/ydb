#pragma once
#include <ydb/library/ycloud/api/user_account_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {

using namespace NKikimr;

struct TUserAccountServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    TUserAccountServiceSettings(TString endpoint, TStringBuf userAgentHint);
};

IActor* CreateUserAccountService(const TUserAccountServiceSettings& settings);

inline IActor* CreateUserAccountService(TString endpoint, TStringBuf userAgentHint) {
    TUserAccountServiceSettings settings(std::move(endpoint), userAgentHint);
    return CreateUserAccountService(settings);
}

}

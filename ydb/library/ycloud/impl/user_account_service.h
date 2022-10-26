#pragma once
#include <ydb/library/ycloud/api/user_account_service.h>
#include "grpc_service_client.h"

namespace NCloud {

using namespace NKikimr;

struct TUserAccountServiceSettings : TGrpcClientSettings {};

IActor* CreateUserAccountService(const TUserAccountServiceSettings& settings);

inline IActor* CreateUserAccountService(const TString& endpoint) {
    TUserAccountServiceSettings settings;
    settings.Endpoint = endpoint;
    return CreateUserAccountService(settings);
}

}

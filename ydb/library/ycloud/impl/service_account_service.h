#pragma once
#include <ydb/library/ycloud/api/service_account_service.h>
#include "grpc_service_client.h"

namespace NCloud {

using namespace NKikimr;

struct TServiceAccountServiceSettings : TGrpcClientSettings {};

IActor* CreateServiceAccountService(const TServiceAccountServiceSettings& settings);

inline IActor* CreateServiceAccountService(const TString& endpoint) {
    TServiceAccountServiceSettings settings;
    settings.Endpoint = endpoint;
    return CreateServiceAccountService(settings);
}

}

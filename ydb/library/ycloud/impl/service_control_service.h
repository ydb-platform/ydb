#pragma once
#include <ydb/library/ycloud/api/service_control_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TServiceControlServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    TServiceControlServiceSettings(TString endpoint, TStringBuf userAgentHint);
};

// Actor client of yandex.cloud.priv.iam.v1.ServiceControlService (SetupDelegation / RevokeDelegation).
IActor* CreateServiceControlService(const TServiceControlServiceSettings& settings);

inline IActor* CreateServiceControlService(TString endpoint, TStringBuf userAgentHint) {
    TServiceControlServiceSettings settings(std::move(endpoint), userAgentHint);
    return CreateServiceControlService(settings);
}

}

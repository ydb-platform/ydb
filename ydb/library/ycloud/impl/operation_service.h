#pragma once
#include <ydb/library/ycloud/api/operation_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TOperationServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    TOperationServiceSettings(TString endpoint, TStringBuf userAgentHint);
};

// Actor client of yandex.cloud.priv.iam.v1.OperationService (Get).
IActor* CreateOperationService(const TOperationServiceSettings& settings);

inline IActor* CreateOperationService(TString endpoint, TStringBuf userAgentHint) {
    TOperationServiceSettings settings(std::move(endpoint), userAgentHint);
    return CreateOperationService(settings);
}

}

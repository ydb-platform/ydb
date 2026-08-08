#pragma once
#include <ydb/library/ycloud/api/folder_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TFolderServiceSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateFolderService(const TFolderServiceSettings& settings);

inline IActor* CreateFolderService(TString endpoint, TString userAgentHint) {
    TFolderServiceSettings settings;
    settings.Endpoint = std::move(endpoint);
    settings.UserAgentHint = std::move(userAgentHint);
    return CreateFolderService(settings);
}

IActor* CreateFolderServiceWithCache(const TFolderServiceSettings& settings); // for compatibility with older code

}

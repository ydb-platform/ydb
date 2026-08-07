#pragma once
#include <ydb/library/ycloud/api/folder_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TFolderServiceSettings : NGrpcActorClient::TGrpcClientSettings {
    using NGrpcActorClient::TGrpcClientSettings::TGrpcClientSettings;
};

IActor* CreateFolderService(const TFolderServiceSettings& settings);

inline IActor* CreateFolderService(const TString& endpoint, const TString& userAgentHint) {
    TFolderServiceSettings settings(userAgentHint);
    settings.Endpoint = endpoint;
    return CreateFolderService(settings);
}

IActor* CreateFolderServiceWithCache(const TFolderServiceSettings& settings); // for compatibility with older code

}

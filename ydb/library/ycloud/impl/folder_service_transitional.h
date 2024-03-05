#pragma once
#include <ydb/library/ycloud/api/folder_service_transitional.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

namespace NCloud {

using namespace NKikimr;

struct TFolderServiceTransitionalSettings : NGrpcActorClient::TGrpcClientSettings {};

IActor* CreateFolderServiceTransitional(const TFolderServiceTransitionalSettings& settings);

inline IActor* CreateFolderServiceTransitional(const TString& endpoint) {
    TFolderServiceTransitionalSettings settings;
    settings.Endpoint = endpoint;
    return CreateFolderServiceTransitional(settings);
}

IActor* CreateFolderServiceTransitionalWithCache(const TFolderServiceTransitionalSettings& settings); // for compatibility with older code

}

#pragma once
#include <ydb/library/ycloud/api/folder_service.h>
#include "grpc_service_settings.h"

namespace NCloud {

using namespace NKikimr;

struct TFolderServiceSettings : TGrpcClientSettings {};

IActor* CreateFolderService(const TFolderServiceSettings& settings);

inline IActor* CreateFolderService(const TString& endpoint) {
    TFolderServiceSettings settings;
    settings.Endpoint = endpoint;
    return CreateFolderService(settings);
}

IActor* CreateFolderServiceWithCache(const TFolderServiceSettings& settings); // for compatibility with older code

}

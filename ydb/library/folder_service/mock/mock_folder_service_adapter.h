#pragma once

#include <ydb/library/folder_service/proto/config.pb.h>

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NFolderService {

NActors::IActor* CreateMockFolderServiceAdapterActor(const NKikimrProto::NFolderService::TFolderServiceConfig& config);
}

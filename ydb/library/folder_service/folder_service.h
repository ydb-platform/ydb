#pragma once

#include <ydb/library/folder_service/proto/config.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NFolderService {

NActors::TActorId FolderServiceActorId();

NActors::IActor* CreateFolderServiceActor(const NKikimrProto::NFolderService::TFolderServiceConfig& config);

} // namespace NKikimr::NFolderService

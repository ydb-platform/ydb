#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NBackup {

using namespace NActors;

IActor* CreateBackupControllerTablet(const TActorId& tablet, TTabletStorageInfo* info);

} // namespace NKikimr::NBackup

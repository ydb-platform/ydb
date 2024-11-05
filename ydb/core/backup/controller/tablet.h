#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NBackup {

using namespace NActors;

IActor* CreateBackupController(const TActorId& tablet, TTabletStorageInfo* info);

} // namespace NKikimr::NBackup

#pragma once

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/library/actors/core/actor.h>

#include <util/generic/string.h>

namespace NKikimr {

NActors::IActor* CreateRocksDiskActor(const TString& dbPath, const TVDiskID& vdiskId, ui64 incarnationGuid = 1);

} // namespace NKikimr

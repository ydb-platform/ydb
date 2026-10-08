#pragma once

#include "runtime.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>

#include <functional>

namespace NKikimr {

const TBlobStorageGroupType::EErasureSpecies BootGroupErasure = TBlobStorageGroupType::ErasureNone;

TTabletStorageInfo* CreateTestTabletInfo(ui64 tabletId, TTabletTypes::EType tabletType,
    TBlobStorageGroupType::EErasureSpecies erasure = BootGroupErasure, ui32 groupId = 0);
TActorId CreateTestBootstrapper(TTestActorRuntime& runtime, TTabletStorageInfo* info,
    std::function<IActor* (const TActorId&, TTabletStorageInfo*)> op, ui32 nodeIndex = 0);
TActorId StartTestTablet(TTestActorRuntime& runtime, TTabletStorageInfo* info,
    std::function<IActor* (const TActorId&, TTabletStorageInfo*)> op, ui32 nodeIndex = 0);
NTabletPipe::TClientConfig GetPipeConfigWithRetries();

} // namespace NKikimr

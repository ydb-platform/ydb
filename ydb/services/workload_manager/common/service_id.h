#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NWorkloadManager {

NActors::TActorId MakeServiceId(ui32 nodeId);

}   // namespace NKikimr::NWorkloadManager

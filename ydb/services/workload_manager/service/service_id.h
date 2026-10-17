#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NWorkloadManager {

inline NActors::TActorId MakeServiceId(ui32 nodeId) {
    const char name[12] = "kqp_workld";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

}  // namespace NKikimr::NWorkloadManager

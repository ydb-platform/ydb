#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NGRpcProxy::V1 {

inline NActors::TActorId GetPQWriteServiceActorID() {
    return NActors::TActorId(0, "PQWriteSvc");
}

} // namespace NKikimr::NGRpcProxy::V1

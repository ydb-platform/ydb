#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NYql {
namespace NDq {

inline static NActors::TActorId MakeCheckpointStorageID() {
    const char name[12] = "cp_storage";
    return NActors::TActorId(0, TStringBuf(name, 12));
}

} // namespace NDq
} // namespace NYql

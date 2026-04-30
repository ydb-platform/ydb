#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NSecrets {

inline NActors::TActorId MakeDescribeSchemaSecretServiceId(ui32 nodeId) {
    const char name[12] = "kqp_dsc_sec";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

}  // namespace NKikimr::NSecrets

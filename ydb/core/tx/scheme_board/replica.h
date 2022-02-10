#pragma once

#include "defs.h"

#include <ydb/core/base/statestorage.h>

namespace NKikimr {

// same as MakeStateStorageReplicaID
inline TActorId MakeSchemeBoardReplicaID(
    const ui32 node,
    const ui64 stateStorageGroup,
    const ui32 replicaIndex
) {
    char x[12] = { 's', 'b', 's' };
    x[3] = (char)stateStorageGroup;
    memcpy(x + 5, &replicaIndex, sizeof(ui32));
    return TActorId(node, TStringBuf(x, 12));
}

} // NKikimr

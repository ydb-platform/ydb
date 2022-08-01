#pragma once

#include "defs.h"
#include <util/generic/string.h>

namespace NKikimr {

inline TActorId MakeDataShardLoadId(ui32 nodeId) {
    char x[12] = {'d', 's', 'l', 'o', 'a', 'd', 'd', 0};
    x[8] = (char)(nodeId >> 24);
    x[9] = (char)(nodeId >> 16);
    x[10] = (char)(nodeId >> 8);
    x[11] = (char)nodeId;
    return TActorId(nodeId, TStringBuf(x, 12));
}

} // NKikimr

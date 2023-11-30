#pragma once

#include <ydb/library/actors/core/actor.h>
#include "interconnect_common.h"

namespace NInterconnect {

    NActors::IActor *CreateInterconnectMonActor(TIntrusivePtr<NActors::TInterconnectProxyCommon> common = nullptr);

    static inline NActors::TActorId MakeInterconnectMonActorId(ui32 nodeId) {
        char s[12] = {'I', 'C', 'O', 'v', 'e', 'r', 'v', 'i', 'e', 'w', 0, 0};
        return NActors::TActorId(nodeId, TStringBuf(s, 12));
    }

} // NInterconnect

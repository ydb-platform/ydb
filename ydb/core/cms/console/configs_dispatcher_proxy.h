#pragma once

#include "defs.h"
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NConsole {

IActor* CreateConfigsDispatcherProxy();

inline TActorId MakeConfigsDispatcherProxyID(ui32 node = 0) {
    char x[12] = { 'c', 'o', 'n', 'f', 'p', 'r', 'o', 'x', 'y', 'd', 's', 'p' };
    return TActorId(node, TStringBuf(x, 12));
}

} // namespace NKikimr::NConsole 
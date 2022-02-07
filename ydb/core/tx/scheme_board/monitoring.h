#pragma once

#include "defs.h"

namespace NKikimr {

inline TActorId MakeSchemeBoardMonitoringId() {
    char x[12] = { 's', 'b', 'm', 'o', 'n' };
    return TActorId(0, TStringBuf(x, 12));
}

IActor* CreateSchemeBoardMonitoring();

} // NKikimr

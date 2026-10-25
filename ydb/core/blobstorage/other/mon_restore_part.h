#pragma once

#include "defs.h"

namespace NKikimr {

    inline TActorId MakeMonRestorePartId() {
        return TActorId(0, TStringBuf("restore_part", 12));
    }

    IActor* CreateMonRestorePartActor();

} // NKikimr

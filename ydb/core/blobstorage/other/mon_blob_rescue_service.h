#pragma once

#include "defs.h"

namespace NKikimr {

    inline TActorId MakeMonBlobRescueServiceId() {
        return TActorId(0, TStringBuf("blob_rescue", 11));
    }

    IActor* CreateMonBlobRescueServiceActor();

} // NKikimr

#pragma once

#include "defs.h"

#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NKikimr {

    inline TActorId MakeMonBlobRangeId() {
        return TActorId(0, TStringBuf("blob_range_m", 12));
    }

    IActor *CreateMonBlobRangeActor();

} // NKikimr

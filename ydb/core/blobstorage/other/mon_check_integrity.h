#pragma once

#include "defs.h"

#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NKikimr {

    inline TActorId MakeMonCheckIntegrityId() {
        return TActorId(0, TStringBuf("check_integ", 11));
    }

    IActor *CreateMonCheckIntegrityActor();

} // NKikimr

#pragma once

#include "defs.h"

#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NKikimr {

    inline TActorId MakeMonGetBlobId() {
        return TActorId(0, TStringBuf("get_blob_mon", 12));
    }

    IActor *CreateMonGetBlobActor();

} // NKikimr

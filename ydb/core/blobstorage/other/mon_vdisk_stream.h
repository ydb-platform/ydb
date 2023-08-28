#pragma once

#include "defs.h"

#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NKikimr {

    static TActorId MakeMonVDiskStreamId() {
        return TActorId(0, TStringBuf("vdisk_stream", 12));
    }

    IActor *CreateMonVDiskStreamActor();

} // NKikimr

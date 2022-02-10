#pragma once

#include "defs.h"

namespace NKikimr {

    // Some services to run on a node for correct functionalily of VDisks

    extern IActor *CreateReplBrokerActor(ui64 maxMemBytes);

} // NKikimr


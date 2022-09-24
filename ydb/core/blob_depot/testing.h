#pragma once

#include "defs.h"

namespace NKikimr::NTesting {

    class TGroupOverseer;

} // NKikimr::NTesting

namespace NKikimr::NBlobDepot {

    bool IsBlobDepotActor(IActor *actor);
    void ValidateBlobDepot(IActor *actor, NTesting::TGroupOverseer& overseer);

} // NKikimr::NBlobDepot

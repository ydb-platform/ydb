#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    IActor *CreateBlobDepotAgent(ui32 virtualGroupId);

} // NKikimr::NBlobDepot

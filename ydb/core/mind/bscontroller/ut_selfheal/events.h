#pragma once

#include "defs.h"

enum {
    EvCheckState = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
    EvDone,
    EvUpdateDriveStatus,
    EvArmTimer,
    EvTimer,
};

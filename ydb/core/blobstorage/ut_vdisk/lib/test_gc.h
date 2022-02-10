#pragma once

#include "defs.h"
#include "prepare.h"


#define SIMPLE_CLASS_DEF_NO_PARAMS(name)    \
struct name {                               \
    void operator ()(TConfiguration *conf); \
};

SIMPLE_CLASS_DEF_NO_PARAMS(TGCPutBarrierVDisk0)
SIMPLE_CLASS_DEF_NO_PARAMS(TGCPutBarrier)
SIMPLE_CLASS_DEF_NO_PARAMS(TGCPutKeepBarrier)
SIMPLE_CLASS_DEF_NO_PARAMS(TGCPutKeepIntoEmptyDB)
SIMPLE_CLASS_DEF_NO_PARAMS(TGCManyVPutsCompactGCAll)
SIMPLE_CLASS_DEF_NO_PARAMS(TGCManyVPutsDelTablet)
SIMPLE_CLASS_DEF_NO_PARAMS(TGCPutManyBarriers)

#undef SIMPLE_CLASS_DEF_NO_PARAMS

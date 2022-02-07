#pragma once

#include "defs.h"
#include "prepare.h"

///////////////////////////////////////////////////////////////////////////
struct TWriteUntilDeviceDeath {
    void operator ()(TConfiguration *conf);
};


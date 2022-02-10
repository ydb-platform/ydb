#pragma once

#include "defs.h"
#include "prepare.h"

///////////////////////////////////////////////////////////////////////////
struct TAllVDisksParallelWrite {
    void operator ()(TConfiguration *conf);
};

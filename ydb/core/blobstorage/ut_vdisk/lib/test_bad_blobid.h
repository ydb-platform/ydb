#pragma once

#include "defs.h"
#include "prepare.h"

///////////////////////////////////////////////////////////////////////////
struct TWriteAndExpectError {
    void operator ()(TConfiguration *conf);
};


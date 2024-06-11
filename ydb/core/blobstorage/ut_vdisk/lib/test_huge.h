#pragma once

#include "defs.h"
#include "prepare.h"

struct THugeModuleTest {
    void operator ()(TConfiguration *conf);
};

struct THugeAllocTest {
    void operator ()(TConfiguration *conf);
};
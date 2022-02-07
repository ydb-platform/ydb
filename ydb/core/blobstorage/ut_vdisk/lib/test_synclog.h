#pragma once

#include "defs.h"
#include "prepare.h"

struct TSyncLogTest {
    void Run();
};

struct TSyncLogTestWrite {
    void operator ()(TConfiguration *conf);
};

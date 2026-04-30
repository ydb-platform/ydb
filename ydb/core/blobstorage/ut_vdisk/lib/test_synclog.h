#pragma once

#include "defs.h"
#include "prepare.h"

struct TSyncLogTest {
    void Run();
};

struct TSyncLogTestWrite {
    void operator ()(TConfiguration *conf);
};

struct TSyncLogCutLogProgress {
    void operator ()(TConfiguration *conf);
};

struct TSyncLogSeedNormalVDiskBeforeRescue {
    void operator ()(TConfiguration *conf);
};

struct TSyncLogRescueWriteGateE2E {
    void operator ()(TConfiguration *conf);
};

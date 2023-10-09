#pragma once

#include <util/datetime/base.h>
#include <util/system/yassert.h>

#define UNIT_WAIT_FOR(condition)                                                     \
    do {                                                                             \
        TInstant start(TInstant::Now());                                             \
        while (!(condition) && (TInstant::Now() - start < TDuration::Seconds(10))) { \
            Sleep(TDuration::MilliSeconds(1));                                       \
        }                                                                            \
        /* TODO: use UNIT_ASSERT if in unittest thread */                            \
        Y_ABORT_UNLESS(condition, "condition failed after 10 seconds wait");               \
    } while (0)

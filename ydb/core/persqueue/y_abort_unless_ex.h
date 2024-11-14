#pragma once

#include <util/datetime/base.h>

#define Y_ABORT_UNLESS_EX(expr, ...)    \
    if (!(expr)) {                      \
        Sleep(TDuration::Seconds(5));   \
    }                                   \
    Y_ABORT_UNLESS(expr, __VA_ARGS__)

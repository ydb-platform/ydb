#pragma once

#include <util/datetime/base.h>

#include <util/random/random.h>

/*
 * returns random duration in range [0, max]
 */
TDuration RandomDuration(TDuration max);

#pragma once

#include <util/generic/string.h>

/**
 * Returns microseconds since the epoch.
 */
ui64 ParseTime(const TString& str, ui64 defValue, int offset /* timezone offset, default MSK */ = 3);

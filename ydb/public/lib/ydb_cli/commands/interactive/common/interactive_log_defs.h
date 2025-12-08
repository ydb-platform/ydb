#pragma once

#include "interactive_log.h"

#define YDB_CLI_LOG(level, stream)                        \
    if (auto entry = Log.level(); entry.LogEnabled()) { \
        entry << stream;                                  \
    }

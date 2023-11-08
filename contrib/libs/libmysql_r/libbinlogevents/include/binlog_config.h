#pragma once

#if defined(__linux__)
#include "binlog_config-linux.h"
#endif

#if defined(__APPLE__)
#include "binlog_config-osx.h"
#endif

#if defined(_MSC_VER)
#include "binlog_config-win.h"
#endif

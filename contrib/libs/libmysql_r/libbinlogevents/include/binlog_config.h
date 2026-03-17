#pragma once

#if defined(__APPLE__)
#   include "binlog_config-osx.h"
#elif defined(_MSC_VER)
#   include "binlog_config-win.h"
#else
#   include "binlog_config-linux.h"
#endif

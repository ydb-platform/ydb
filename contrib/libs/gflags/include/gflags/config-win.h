#pragma once

#include "config-linux.h"

#define OS_WINDOWS
#undef HAVE_UNISTD_H
#undef HAVE_FNMATCH_H
#define HAVE_SHLWAPI_H
#undef HAVE_STRTOLL
#undef HAVE_PTHREAD
#undef HAVE_RWLOCK

#include "windows_port.h"

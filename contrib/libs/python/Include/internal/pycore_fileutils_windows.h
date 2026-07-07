#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_fileutils_windows.h>
#else
#error "No <internal/pycore_fileutils_windows.h> in Python2"
#endif

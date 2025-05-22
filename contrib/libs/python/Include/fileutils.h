#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/fileutils.h>
#else
#error "No <fileutils.h> in Python2"
#endif

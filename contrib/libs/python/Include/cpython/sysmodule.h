#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/sysmodule.h>
#else
#error "No <cpython/sysmodule.h> in Python2"
#endif

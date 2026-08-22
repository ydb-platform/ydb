#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/sysmodule.h>
#else
#error "No <cpython/sysmodule.h> in Python2"
#endif

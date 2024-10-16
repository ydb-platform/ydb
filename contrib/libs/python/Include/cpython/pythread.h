#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pythread.h>
#else
#error "No <cpython/pythread.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/lock.h>
#else
#error "No <cpython/lock.h> in Python2"
#endif

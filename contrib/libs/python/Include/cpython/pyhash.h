#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pyhash.h>
#else
#error "No <cpython/pyhash.h> in Python2"
#endif

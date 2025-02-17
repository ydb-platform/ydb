#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pyfpe.h>
#else
#error "No <cpython/pyfpe.h> in Python2"
#endif

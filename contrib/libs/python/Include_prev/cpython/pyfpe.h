#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pyfpe.h>
#else
#error "No <cpython/pyfpe.h> in Python2"
#endif

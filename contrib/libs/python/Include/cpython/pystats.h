#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pystats.h>
#else
#error "No <cpython/pystats.h> in Python2"
#endif

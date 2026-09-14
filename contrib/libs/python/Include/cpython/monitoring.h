#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/monitoring.h>
#else
#error "No <cpython/monitoring.h> in Python2"
#endif

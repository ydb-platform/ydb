#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/pybuffer.h>
#else
#error "No <pybuffer.h> in Python2"
#endif

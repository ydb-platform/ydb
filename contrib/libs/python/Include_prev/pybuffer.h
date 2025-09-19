#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/pybuffer.h>
#else
#error "No <pybuffer.h> in Python2"
#endif

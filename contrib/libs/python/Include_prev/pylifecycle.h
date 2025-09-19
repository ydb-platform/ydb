#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/pylifecycle.h>
#else
#error "No <pylifecycle.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/pylifecycle.h>
#else
#error "No <pylifecycle.h> in Python2"
#endif

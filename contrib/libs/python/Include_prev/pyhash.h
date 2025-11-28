#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/pyhash.h>
#else
#error "No <pyhash.h> in Python2"
#endif

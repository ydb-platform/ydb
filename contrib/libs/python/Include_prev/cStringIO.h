#pragma once

#ifdef USE_PYTHON3_PREV
#error "No <cStringIO.h> in Python3"
#else
#include <contrib/tools/python/src/Include/cStringIO.h>
#endif

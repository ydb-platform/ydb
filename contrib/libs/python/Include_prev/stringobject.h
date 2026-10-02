#pragma once

#ifdef USE_PYTHON3_PREV
#error "No <stringobject.h> in Python3"
#else
#include <contrib/tools/python/src/Include/stringobject.h>
#endif

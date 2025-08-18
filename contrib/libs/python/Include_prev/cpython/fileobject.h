#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/fileobject.h>
#else
#error "No <cpython/fileobject.h> in Python2"
#endif

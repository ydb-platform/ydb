#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/odictobject.h>
#else
#error "No <cpython/odictobject.h> in Python2"
#endif

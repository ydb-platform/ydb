#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/objimpl.h>
#else
#error "No <cpython/objimpl.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/fileutils.h>
#else
#error "No <cpython/fileutils.h> in Python2"
#endif
